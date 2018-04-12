module Main where

import Distribution.Server.Features.Users
import Distribution.Server.Features.Core
-- import Distribution.Server.Features.Upload
import qualified Distribution.Client.Index as Index
import qualified Data.ByteString.Lazy as BS
import System.Environment
import qualified Data.Set as S
import Distribution.Types.PackageId
import Codec.Archive.Tar.Entry
import Control.Monad (join)
import qualified Distribution.Server as Server
import qualified Distribution.Verbosity as Verbosity

lastExistingTimestamp= 1523451453 --TODO pick right stamp



main = do
   [fname] <- getArgs
   bs <- BS.readFile fname
   serverEnv <- Server.mkServerEnv =<< Server.defaultServerConfig

   users <- join $ initUserFeature serverEnv
--    core <- ($ userFeature) =<<  initCoreFeature serverEnv
   coreState <- packagesStateComponent Verbosity.normal False (Server.serverStateDir serverEnv)

   let Right allEntries = Index.read (,) (const True) bs -- if we don't parse, we die
       -- entries are reverse chron, but we want to process them oldest to newest
       entries = reverse $ takeWhile (\x -> entryTime (snd x) > lastExistingTimestamp) allEntries

   mapM_ (processEntry entries users coreState) entries


processEntry entries users coreState x = do
  print (fst x)
 {-
-- things can be preferred, json, or package
-- if json, ignore (we'll add with package)
if package then:

check if user exists, else add
check if package exists at version
  if yes -- upload revision -- simple
  if no -- lookup package.json in entries
           fetch tarball from remote (make sure is pristine)

--nb this is manual and not through core feature to ensure we can add the json entry manually and precisely
updateState coreState $
        AddPackage3
          pkginfo
          uploadinfo
          (userName userInfo)
          additionalEntries

If preferred then??

-}



--  fst is pkgid, snd is entry
{-

entries are reverse chronological
we just take while > some amount

Logic is as follows:

A) filter data to only be sufficiently recent
B) for each package, add it to the db with a virtual upload at the correct time and add the correct package.json
C) for each user if they don't exist, add them with the correct id


-}

--   mapM print $ (map ((\x -> x {entryContent=Directory}) . snd) . take 100) entries
