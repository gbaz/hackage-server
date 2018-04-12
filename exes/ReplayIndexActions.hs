module Main where

import Distribution.Server.Features.Users
import qualified Distribution.Server.Users.Users as Users
import Distribution.Server.Users.State
import Distribution.Server.Features.Core
import Distribution.Server.Features.Core.State
import qualified Distribution.Client.Index as Index
import qualified Data.ByteString.Lazy as BS
import System.Environment
import Distribution.Types.PackageId
import Codec.Archive.Tar.Entry
import Control.Monad (join)
import qualified Distribution.Server as Server
import qualified Distribution.Verbosity as Verbosity
import Distribution.Server.Framework.Feature (updateState, queryState, StateComponent(..))
import Distribution.Server.Packages.Types
import qualified Data.Vector as V
import Distribution.Server.Users.Users(lookupUserId)
import Distribution.Server.Users.Types
import Data.Time.Clock.POSIX
import qualified Distribution.Server.Framework.BlobStorage as BlobStorage
import qualified Distribution.Server.Util.GZip as GZip
import Data.Acid.Abstract
import Data.List(isSuffixOf)
import Distribution.Server.Packages.Index
import qualified Distribution.Server.Packages.PackageIndex as PI
import Data.Maybe(isJust)
import qualified Data.Map as M


lastExistingTimestamp :: EpochTime
lastExistingTimestamp = 1523451453 --TODO pick right stamp

main :: IO ()
main = do
   [fname] <- getArgs
   bs <- BS.readFile fname
   --nb turns out we really only need the statedir and blobdir from here, so overkill...
   serverEnv <- Server.mkServerEnv . (\x -> x {Server.confStaticDir="datafiles"}) =<< Server.defaultServerConfig

   userState <- usersStateComponent (Server.serverStateDir serverEnv)
   coreState <- packagesStateComponent Verbosity.normal False (Server.serverStateDir serverEnv)

   let Right allEntries = Index.read (,) (const True) bs -- if we don't parse, we die
       -- entries are reverse chron, but we want to process them oldest to newest
       entries = reverse $ take 1000 allEntries
       -- entries = reverse $ takeWhile (\x -> entryTime (snd x) > lastExistingTimestamp) allEntries -- TODO reenable

   mapM_ (processEntry userState coreState (Server.serverBlobStore serverEnv)) entries

-- Note this doesn't handle preferred entries yet TODO

processEntry :: StateComponent AcidState Users.Users
               -> StateComponent AcidState PackagesState
               -> BlobStorage.BlobStorage
               -> (PackageIdentifier, Entry)
               -> IO ()
processEntry userState coreState blobstore x = do
  print (fst x)
--  print $  (snd x) {entryContent=Directory}
  let
     doPackageUpload :: IO ()
     doPackageUpload = do
       (uid,userInfo) <- userInsertOrLookup
       pkgTarball <- createPackageTarball blobstore (fst x)
       let ultime = posixSecondsToUTCTime $ realToFrac $ entryTime (snd x)
           ulinfo = (ultime, uid)
       NormalFile bs _ <- return $ entryContent (snd x)
       updateState coreState $ AddPackage3
          (PkgInfo
            (fst x)
            (V.fromList [(CabalFileText bs,ulinfo)]) -- list of (CabalFileText,UploadInfo)
            (V.fromList [(pkgTarball,ulinfo)]) -- list of (PkgTarball, UploadInfo)
          )
          (ultime,uid)
          (userName userInfo)
          [MetadataEntry (fst x) 0 ultime] -- this gives the package.json
       return ()

     doPackageRevision :: IO ()
     doPackageRevision = do
       (uid,userInfo) <- userInsertOrLookup
       let ultime = posixSecondsToUTCTime $ realToFrac $ entryTime (snd x)
           ulinfo = (ultime, uid)
       NormalFile bs _ <- return $ entryContent (snd x)
       updateState coreState $ AddPackageRevision2
          (fst x)
          (CabalFileText bs)
          ulinfo
          (userName userInfo)
       return ()

     userInsertOrLookup :: IO (UserId,UserInfo)
     userInsertOrLookup = do
        let uname = ownerName $ entryOwnership (snd x)
            uid   = UserId $ ownerId $ entryOwnership (snd x)

        usersdb <- queryState userState $ GetUserDb
        case lookupUserId uid usersdb of
          Just userInfo -> return (uid,userInfo)
          Nothing ->
               let uinfo = UserInfo (UserName uname) (AccountEnabled (UserAuth (PasswdHash "BADHASH"))) M.empty
               in case (Users.insertUserAccount uid uinfo) usersdb of
                          Left err -> print ("user insert error: " ++ show (uid, uinfo)) >> return (uid, uinfo)
                          Right newdb -> updateState userState (ReplaceUserDb newdb) >> return (uid, uinfo)

  pkgIndex <- fmap packageIndex . queryState coreState $ GetPackagesState
  let packageVersionAlreadyExists = isJust $ PI.lookupPackageId pkgIndex (fst x)
  let isPackageJson = "package.json" `isSuffixOf` fromTarPathToPosixPath (entryTarPath (snd x))
  if isPackageJson
    then print "ignoring packagejson" --return () -- nothing to be done
    else
      if packageVersionAlreadyExists
        then print "isrevision"   -- >> doPackageRevision
        else print "isupload" -- >> doPackageUpload
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
