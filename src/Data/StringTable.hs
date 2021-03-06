{-# LANGUAGE CPP, DeriveDataTypeable, TemplateHaskell #-}

module Data.StringTable (

    StringTable,
    lookup,
    index,
    construct,

    getTable,
    putTable,

#if TEST    
    prop,
#endif
  
 ) where

import Prelude hiding (lookup)

import Control.Applicative
import qualified Data.Binary as B
import qualified Data.List as List
import qualified Data.Array.Unboxed as A
import Data.Array.Unboxed ((!))
import Data.Typeable (Typeable)
import qualified Data.ByteString.Char8 as BS
import Data.Word (Word32)

-- | An effecient mapping from strings to a dense set of integers.
--
data Enum id => StringTable id
              = StringTable
                  !BS.ByteString          -- all the strings concatenated
                  !(A.UArray Int Word32)  -- offset table
  deriving (Show, Typeable)


-- Using Data.Binary UArray instances, although I don't like creating
-- a UArray via a list.
putTable :: StringTable id -> B.Put
putTable (StringTable bs arr) = B.put bs >> B.put arr

getTable :: Enum id => B.Get (StringTable id)
getTable = StringTable <$> B.get <*> B.get

-- | Look up a string in the token table. If the string is present, return
-- its corresponding index.
--
lookup :: Enum id => StringTable id -> String -> Maybe id
lookup (StringTable bs tbl) str = binarySearch 0 (topBound-1) (BS.pack str)
  where
    (0, topBound) = A.bounds tbl

    binarySearch a b key
      | a > b     = Nothing
      | otherwise = case compare key (index' bs tbl mid) of
          LT -> binarySearch a (mid-1) key
          EQ -> Just (toEnum mid)
          GT -> binarySearch (mid+1) b key
      where mid = (a + b) `div` 2
    
index' :: BS.ByteString -> A.UArray Int Word32 -> Int -> BS.ByteString
index' bs tbl i = BS.take len . BS.drop start $ bs
  where
    start, end, len :: Int
    start = fromIntegral (tbl ! i)
    end   = fromIntegral (tbl ! (i+1))
    len   = end - start


-- | Given the index of a string in the table, return the string.
--
index :: Enum id => StringTable id -> id -> String
index (StringTable bs tbl) = BS.unpack . index' bs tbl . fromEnum


-- | Given a list of strings, construct a 'StringTable' mapping those strings
-- to a dense set of integers.
--
construct :: Enum id => [String] -> StringTable id
construct strs = StringTable bs tbl
  where
    bs      = BS.pack (concat strs')
    tbl     = A.array (0, length strs') (zip [0..] offsets)
    offsets = scanl (\off str -> off + fromIntegral (length str)) 0 strs'
    strs'   = map head . List.group . List.sort $ strs


enumStrings :: Enum id => StringTable id -> [String]
enumStrings (StringTable bs tbl) = map (BS.unpack . index' bs tbl) [0..h-1]
  where (0,h) = A.bounds tbl


enumIds :: Enum id => StringTable id -> [id]
enumIds (StringTable _ tbl) = map toEnum [0..h-1]
  where (0,h) = A.bounds tbl

#if TEST
prop :: [String] -> Bool
prop strs =
     all lookupIndex (enumStrings tbl)
  && all indexLookup (enumIds tbl)

  where
    tbl :: StringTable Int
    tbl = construct strs
    
    lookupIndex str = index tbl ident == str
      where Just ident = lookup tbl str
    
    indexLookup ident = lookup tbl str == Just ident
      where str       = index tbl ident
#endif
