{-# LANGUAGE CPP #-}
{-# LANGUAGE GeneralizedNewtypeDeriving, DeriveDataTypeable #-}

module Data.TarIndex (
  
    TarIndex,
    TarIndexEntry(..),
    TarEntryOffset,
      
    lookup,
    construct,
    constructIO,
    index,
    indexFile,

    putTarIndex,
    getTarIndex,

#if TESTS
    prop_lookup, prop,
#endif
  ) where

import Prelude hiding (lookup)

import qualified Codec.Archive.Tar as Tar
import qualified Codec.Archive.Tar.Entry as Tar
import Control.Applicative
import Control.Exception (evaluate)
import Data.Binary
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Lazy.Util as BSL
import qualified Data.ByteString as BS
import Data.Typeable (Typeable)
import Data.Version (showVersion)
import System.IO
import qualified System.FilePath as FilePath

import qualified Data.StringTable as StringTable
import Data.StringTable (StringTable)
import qualified Data.IntTrie as IntTrie
import Data.IntTrie (IntTrie)
import Paths (version)

-- | An index of the entries in a tar file. This lets us look up a filename
-- within the tar file and find out where in the tar file (ie the file offset)
-- that entry occurs.
--
data TarIndex = TarIndex

  -- As an example of how the mapping works, consider these example files:
  --   "foo/bar.hs" at offset 0
  --   "foo/baz.hs" at offset 1024
  --
  -- We split the paths into components and enumerate them.
  --   { "foo" -> TokenId 0, "bar.hs" -> TokenId 1,  "baz.hs" -> TokenId 2 }
  --
  -- We convert paths into sequences of 'TokenId's, i.e.
  --   "foo/bar.hs" becomes [PathComponentId 0, PathComponentId 1]
  --   "foo/baz.hs" becomes [PathComponentId 0, PathComponentId 2]
  --
  -- We use a trie mapping sequences of 'PathComponentId's to the entry offset:
  --  { [PathComponentId 0, PathComponentId 1] -> offset 0
  --  , [PathComponentId 0, PathComponentId 1] -> offset 1024 }

  -- The mapping of filepath components as strings to ids.
  !(StringTable PathComponentId)

  -- Mapping of sequences of filepath component ids to tar entry offsets.
  !(IntTrie PathComponentId TarEntryOffset)
  deriving (Show, Typeable)

-- | An entry in the index is either the offset for the
-- requested file or a sub-directory listing.
data TarIndexEntry = TarFileEntry !TarEntryOffset
                   | TarDir [FilePath]
  deriving (Show, Typeable)

newtype PathComponentId = PathComponentId Int
  deriving (Eq, Ord, Enum, Show, Typeable)

type TarEntryOffset = Int

tarIndexVersion :: Word32
tarIndexVersion = 2

tarIndexInfoStr :: String
tarIndexInfoStr = "This file brought to you by tarindex, version " ++ showVersion version

-- | One-stop shop for reading using a tar index
indexFile
    :: TarIndex -- ^ Tar Index
    -> FilePath -- ^ Entry to read in index
    -> FilePath -- ^ Path to tar file
    -> IO (Either [FilePath] BSL.ByteString)
indexFile tIndex entryPath tarFile
    = case lookup tIndex entryPath of
        Nothing -> return $ Left []
        Just (TarDir paths) -> return $ Left paths
        Just (TarFileEntry offset) -> Right <$> indexFileEntry tarFile offset

indexFileEntry :: FilePath -> TarEntryOffset -> IO BSL.ByteString
indexFileEntry tarfile off
    = do
  htar <- openFile tarfile ReadMode
  hSeek htar AbsoluteSeek (fromIntegral (off * 512))
  header <- BSL.hGet htar 512
  case Tar.read header of
    (Tar.Next Tar.Entry{Tar.entryContent = Tar.NormalFile _ size} _) -> 
        BSL.hGetSomeContents htar (fromIntegral size)
    _ -> hClose htar >> fail "error indexing from tar file"

indexBSEntry :: BS.ByteString -> TarEntryOffset -> BSL.ByteString
indexBSEntry tarbytes off
    = let entryBytes = BS.drop (off * 512) $ tarbytes
      in case Tar.read (BSL.fromChunks [entryBytes]) of
    (Tar.Next Tar.Entry{Tar.entryContent = Tar.NormalFile body size} _) -> 
        body
    _ -> error "error indexing from tar file"

index :: TarIndex -> FilePath -> BS.ByteString -> Maybe (Either [FilePath] BSL.ByteString)
index tIndex entryPath tarBytes
    = case lookup tIndex entryPath of
        Nothing -> Nothing
        Just (TarDir paths) -> Just $ Left paths
        Just (TarFileEntry offset) -> Just $ Right $ indexBSEntry tarBytes offset

-- | Serialize a tar index.
putTarIndex :: TarIndex -> Put
putTarIndex (TarIndex table trie)
    = sequence_
      [ put tarIndexVersion  -- 4 byte version tag
      , put tarIndexInfoStr  -- length ptrfixed UTF-8 informational string
      , StringTable.putTable table -- string table
      , IntTrie.putTrie trie  -- trie
      ]

-- | De-serialize a tar index. May fail on version mismatch. When this function
-- returns in 'Left' we do not consume the rest of the bytes associated with the
-- index.
getTarIndex :: Get (Either String TarIndex)
getTarIndex = do
  ver <- get
  if ver /= tarIndexVersion then return (versionError ver) else do
  _ <- get :: Get String
  ind <- TarIndex <$> StringTable.getTable <*> IntTrie.getTrie
  return $ Right ind
 where versionError ver
           = Left $ "Version mis-match in index files, I can't decode a version " ++ show ver ++ " index."

-- | Look up a given filepath in the index. It may return a 'TarFileEntry'
-- containing the offset and length of the file within the tar file, or if
-- the filepath identifies a directory then it returns a 'TarDir' containing
-- the list of files within that directory.
--
lookup :: TarIndex -> FilePath -> Maybe TarIndexEntry
lookup (TarIndex pathTable pathTrie) path =
    case toComponentIds pathTable path of
      Nothing    -> Nothing
      Just fpath -> fmap (mkIndexEntry fpath) (IntTrie.lookup pathTrie fpath)
  where
    mkIndexEntry _ (IntTrie.Entry offset)        = TarFileEntry offset
    mkIndexEntry _ (IntTrie.Completions entries) =
      TarDir [ fromComponentIds pathTable [entry]
             | entry <- entries ]

-- | Construct a tar-index for a file on disk.
-- Failure is indicated by throwing an error.
constructIO :: FilePath -> IO TarIndex
constructIO file = do
  tar <- BSL.readFile file
  let entries = Tar.read tar
  case extractInfo entries of
    info -> evaluate (construct info)

-- | Fails in IO!
extractInfo :: Tar.Entries -> [(FilePath, Int)]
extractInfo = go 0 []
  where
    go _ es' (Tar.Done)      = es'
    go _ _   (Tar.Fail err)  = error err
    go n es' (Tar.Next e es) = go n' ((Tar.entryPath e, n) : es') es
      where
        n' = n + 1
               + case Tar.entryContent e of
                   Tar.NormalFile     _   size -> blocks size
                   Tar.OtherEntryType _ _ size -> blocks size
                   _                           -> 0
        blocks s = 1 + ((fromIntegral s - 1) `div` 512)

-- | Construct a 'TarIndex' from a list of filepaths and their corresponding
-- offset within the tar-file
construct :: [(FilePath, TarEntryOffset)] -> TarIndex
construct pathsOffsets = TarIndex pathTable pathTrie
  where
    pathComponents = concatMap (FilePath.splitDirectories . fst) pathsOffsets
    pathTable = StringTable.construct pathComponents
    pathTrie  = IntTrie.construct
                  [ (cids, offset)
                  | (path, offset) <- pathsOffsets
                  , let Just cids = toComponentIds pathTable path ]

toComponentIds :: StringTable PathComponentId -> FilePath -> Maybe [PathComponentId]
toComponentIds table = lookupComponents [] . FilePath.splitDirectories
  where
    lookupComponents cs' []     = Just (reverse cs')
    lookupComponents cs' (c:cs) = case StringTable.lookup table c of
      Nothing  -> Nothing
      Just cid -> lookupComponents (cid:cs') cs
      
fromComponentIds :: StringTable PathComponentId -> [PathComponentId] -> FilePath
fromComponentIds table = FilePath.joinPath . map (StringTable.index table)

#if TESTS

-- properties of a finite mapping...

prop_lookup :: [(FilePath, TarEntryOffset)] -> FilePath -> Bool
prop_lookup xs x =
  case (lookup (construct xs) x, Prelude.lookup x xs) of
    (Nothing,                    Nothing)      -> True
    (Just (TarFileEntry offset), Just offset') -> offset == offset'
    _                                          -> False

prop :: [(FilePath, TarEntryOffset)] -> Bool
prop paths
  | not $ StringTable.prop pathbits = error "TarIndex: bad string table"
  | not $ IntTrie.prop intpaths     = error "TarIndex: bad int trie"
  | not $ prop'                     = error "TarIndex: bad prop"
  | otherwise                       = True

  where
    index@(TarIndex pathTable _) = construct paths

    pathbits = concatMap (FilePath.splitDirectories . fst) paths
    intpaths = [ (cids, offset)
               | (path, offset) <- paths
               , let Just cids = toComponentIds pathTable path ]
    prop' = flip all paths $ \(file, offset) ->
      case lookup index file of
        Just (TarFileEntry offset') -> offset' == offset
        _                           -> False


example0 :: [(FilePath, Int)]
example0 =
  [("foo-1.0/foo-1.0.cabal", 512)   -- tar block 1
  ,("foo-1.0/LICENSE",       2048)  -- tar block 4
  ,("foo-1.0/Data/Foo.hs",   4096)] -- tar block 8

#endif
