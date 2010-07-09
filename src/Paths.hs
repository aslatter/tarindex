{-# LANGUAGE CPP #-}

module Paths (
    version,
    getBinDir, getLibDir, getDataDir, getLibexecDir,
    getDataFileName
  ) where

#ifdef CABAL_BUILD

import Paths_tarindex

#else

import Data.Version (Version(..))
import System.Environment (getEnv)
import System.FilePath

version :: Version
version = Version {versionBranch = [0,0,0,0,0], versionTags = ["devbuild"]}

bindir, libdir, datadir, libexecdir :: FilePath

bindir     = "."
libdir     = "."
datadir    = "."
libexecdir = "."

getBinDir, getLibDir, getDataDir, getLibexecDir :: IO FilePath
getBinDir = catch (getEnv "tarindex_bindir") (\_ -> return bindir)
getLibDir = catch (getEnv "tarindex_libdir") (\_ -> return libdir)
getDataDir = catch (getEnv "tarindex_datadir") (\_ -> return datadir)
getLibexecDir = catch (getEnv "tarindex_libexecdir") (\_ -> return libexecdir)

getDataFileName :: FilePath -> IO FilePath
getDataFileName name = do
  dir <- getDataDir
  return (dir </> name)

#endif
