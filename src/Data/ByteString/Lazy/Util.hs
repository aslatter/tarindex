{- |
 Module      : Data.ByteString.Lazy.Util
 Copyright   : (c) Don Stewart 2006
               (c) Duncan Coutts 2006
               (c) Antoine Latter 2010
 License     : BSD-style

 Maintainer  : aslatter@gmail.com
 Stability   : experimental
 Portability : portable

 Variations on functions in the bytestring package, mostly
 copied whole-sale.
-}

module Data.ByteString.Lazy.Util
    ( hGetSomeContents
    ) where

import qualified Data.ByteString as S
import Data.ByteString.Lazy
import Data.ByteString.Lazy.Internal
import System.IO
import System.IO.Unsafe (unsafeInterleaveIO)

-- | Read a portion of handle contents /lazily/ into a 'ByteString'. Chunks
-- are read on demand, in at most @k@-sized chunks. It does not block
-- waiting for a whole @k@-sized chunk, so if less than @k@ bytes are
-- available then they will be returned immediately as a smaller chunk.
--
-- 
--
hGetSomeContentsN :: Int -> Handle -> Int -> IO ByteString
hGetSomeContentsN k h = lazyRead -- TODO close on exceptions
  where
    lazyRead n = unsafeInterleaveIO $ loop n

    loop n | n < 1 = hClose h >> return Empty
    loop n = do
        c <- S.hGetNonBlocking h k
        --TODO: I think this should distinguish EOF from no data available
        -- the underlying POSIX call makes this distincion, returning either
        -- 0 or EAGAIN
        if S.null c
          then do eof <- hIsEOF h
                  if eof then hClose h >> return Empty
                         else hWaitForInput h (-1)
                           >> loop n
          else do let remain = n - S.length c
                      c' | remain < 0 = S.take n c
                         | otherwise  = c
                  cs <- lazyRead remain
                  return (Chunk c' cs)

-- | Read a portion of a handle contents /lazily/ into a 'ByteString'. Chunks
-- are read on demand, using the default chunk size.
--
-- The handle is closed on EOF, or when requested size in bytes is read.
--
hGetSomeContents :: Handle -> Int -> IO ByteString
hGetSomeContents = hGetSomeContentsN defaultChunkSize
