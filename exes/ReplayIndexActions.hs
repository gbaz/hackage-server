module Main where

import Distribution.Server.Features.Users
import Distribution.Server.Features.Core
-- import Distribution.Server.Features.Upload
import qualified Distribution.Client.Index as Index
import qualified Data.ByteString.Lazy as BS
import System.Environment
import Distribution.Types.PackageId
import Codec.Archive.Tar.Entry
import Control.Monad (join)
import qualified Distribution.Server as Server
import qualified Distribution.Verbosity as Verbosity
import Distribution.Server.Features.Core.State
import Distribution.Server.Framework.Feature (updateState, StateComponent(..))
import Distribution.Server.Packages.Types
import qualified Data.Vector as V
import Distribution.Server.Users.Users(lookupUserId)
import Distribution.Server.Users.Types
import Data.Time.Clock.POSIX
import qualified Distribution.Server.Framework.BlobStorage as BlobStorage
import qualified Distribution.Server.Util.GZip as GZip
import Data.Acid.Abstract

lastExistingTimestamp :: EpochTime
lastExistingTimestamp= 1523451453 --TODO pick right stamp

main :: IO ()
main = do
   [fname] <- getArgs
   bs <- BS.readFile fname
   serverEnv <- Server.mkServerEnv . (\x -> x {Server.confStaticDir="datafiles"}) =<< Server.defaultServerConfig

   users <- join $ initUserFeature serverEnv
--    core <- ($ userFeature) =<<  initCoreFeature serverEnv
   coreState <- packagesStateComponent Verbosity.normal False (Server.serverStateDir serverEnv)

   let Right allEntries = Index.read (,) (const True) bs -- if we don't parse, we die
       -- entries are reverse chron, but we want to process them oldest to newest
       entries = reverse $ takeWhile (\x -> entryTime (snd x) > lastExistingTimestamp) allEntries

   mapM_ (processEntry entries users coreState (Server.serverBlobStore serverEnv)) entries

-- Note this doesn't handle preferred entries yet

processEntry :: [(PackageIdentifier,Entry)]
                   -> UserFeature
                   -> Distribution.Server.Framework.Feature.StateComponent
                      AcidState PackagesState
                   -> BlobStorage.BlobStorage
                   -> (PackageIdentifier, Entry)
                   -> IO ()
processEntry entries users coreState blobstore x = do
  print (fst x)
  print $  (snd x) {entryContent=Directory}
  let
     doPackageUpload :: IO ()
     doPackageUpload = do
       (uid,userInfo) <- userInsertOrLookup
       pkgTarball <- createPackageTarball blobstore (fst x) --fetch packagetarball bytestring
       let packagejson = undefined -- find json file in entries
       let ultime = posixSecondsToUTCTime $ realToFrac $ entryTime (snd x)
           ulinfo = (ultime, uid)
       NormalFile bs _ <- return $ entryContent (snd x)
       updateState coreState $ AddPackage3
          (PkgInfo
            (fst x)
            (V.fromList [(CabalFileText bs,ulinfo)]) -- list of (CabalFileText,UploadInfo)
            (V.fromList [(pkgTarball,ulinfo)]) -- list of (PkgTarball, UploadInfo)
          )
          (undefined,uid) -- (utctime,userid)
          (userName userInfo)
          [] --TODO, json entry
       return ()

     doPackageRevision :: IO ()
     doPackageRevision = return () --TODO

     userInsertOrLookup :: IO (UserId,UserInfo)
     userInsertOrLookup = do
        let uname = ownerName $ entryOwnership (snd x)
            uid   = UserId $ ownerId $ entryOwnership (snd x)

        usersdb <- queryGetUserDb users
        case lookupUserId uid usersdb of
          Just userInfo -> return (uid,userInfo)
          Nothing -> return undefined -- todo insert user
  packageVersionAlreadyExists <- return undefined --todo
  let isPackageJson = False --todo
  if isPackageJson
    then return ()
    else
      if packageVersionAlreadyExists
        then doPackageRevision
        else doPackageUpload
  return ()

createPackageTarball :: BlobStorage.BlobStorage -> p -> IO PkgTarball
createPackageTarball store pinfo = do
     fileContent <- undefined -- TODOfetchPackage pinfo
     blobId <- BlobStorage.add store fileContent
     infoGz <- blobInfoFromId store blobId
     let filename = undefined
     let decompressedContent = GZip.decompressNamed filename fileContent
     blobIdDecompressed <- BlobStorage.add store decompressedContent
     return  $ PkgTarball {
                              pkgTarballGz   = infoGz
                            , pkgTarballNoGz = blobIdDecompressed
                          }

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
