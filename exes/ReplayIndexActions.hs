module Main where

import Distribution.Server.Features.Users
import qualified Distribution.Server.Users.Users as Users
import Distribution.Server.Users.State
import Distribution.Server.Features.Core
import Distribution.Server.Features.Core.State
import qualified Data.ByteString.Lazy as BS
import System.Environment
import Distribution.Types.PackageId
import Codec.Archive.Tar.Entry
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
import System.Process

import System.FilePath ((</>),(<.>))
import Distribution.Text(display)
import System.Directory(getHomeDirectory)

import qualified Codec.Archive.Tar       as Tar ( read, Entries(..) )
import qualified Codec.Archive.Tar.Entry as Tar ( Entry(..), entryPath )

import Distribution.Package
import Distribution.Text ( simpleParse )

import Data.ByteString.Lazy (ByteString)
import System.FilePath.Posix ( splitDirectories, normalise )

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

   let Right allEntries = readIndex (,) (const True) bs -- if we don't parse, we die
       -- entries are reverse chron, but we want to process them oldest to newest
       -- entries = reverse $ take 1000 allEntries
       entries = reverse $ takeWhile (\x -> either entryTime (entryTime . snd) x > lastExistingTimestamp) allEntries

   mapM_ (processEntry userState coreState (Server.serverBlobStore serverEnv)) entries

-- Note this doesn't handle preferred entries yet TODO

processEntry :: StateComponent AcidState Users.Users
               -> StateComponent AcidState PackagesState
               -> BlobStorage.BlobStorage
               -> Either Entry (PackageIdentifier, Entry)
               -> IO ()
processEntry userState coreState blobstore (Left entry) = print entry -- todo
processEntry userState coreState blobstore (Right (pkgid,entry)) = do
  print pkgid
--  print $ entry {entryContent=Directory}
  let
     doPackageUpload :: IO ()
     doPackageUpload = do
       (uid,userInfo) <- userInsertOrLookup
       pkgTarball <- createPackageTarball blobstore pkgid
       let ultime = posixSecondsToUTCTime $ realToFrac $ entryTime entry
           ulinfo = (ultime, uid)
       NormalFile bs _ <- return $ entryContent entry
       updateState coreState $ AddPackage3
          (PkgInfo
            pkgid
            (V.fromList [(CabalFileText bs,ulinfo)]) -- list of (CabalFileText,UploadInfo)
            (V.fromList [(pkgTarball,ulinfo)]) -- list of (PkgTarball, UploadInfo)
          )
          (ultime,uid)
          (userName userInfo)
          [MetadataEntry pkgid 0 ultime] -- this gives the package.json
       return ()

     doPackageRevision :: IO ()
     doPackageRevision = do
       (uid,userInfo) <- userInsertOrLookup
       let ultime = posixSecondsToUTCTime $ realToFrac $ entryTime entry
           ulinfo = (ultime, uid)
       NormalFile bs _ <- return $ entryContent entry
       updateState coreState $ AddPackageRevision2
          pkgid
          (CabalFileText bs)
          ulinfo
          (userName userInfo)
       return ()

     userInsertOrLookup :: IO (UserId,UserInfo)
     userInsertOrLookup = do
        let uname = ownerName $ entryOwnership entry
            uid   = UserId $ ownerId $ entryOwnership entry

        usersdb <- queryState userState $ GetUserDb
        case lookupUserId uid usersdb of
          Just userInfo -> return (uid,userInfo)
          Nothing ->
               let uinfo = UserInfo (UserName uname) (AccountEnabled (UserAuth (PasswdHash "BADHASH"))) M.empty
               in case (Users.insertUserAccount uid uinfo) usersdb of
                          Left err -> print ("user insert error: " ++ show (uid, uinfo)) >> return (uid, uinfo)
                          Right newdb -> updateState userState (ReplaceUserDb newdb) >> return (uid, uinfo)

  pkgIndex <- fmap packageIndex . queryState coreState $ GetPackagesState
  let existingPkg = PI.lookupPackageId pkgIndex pkgid
      packageVersionAlreadyExists = isJust $ existingPkg
      replayAlreadyDone = case existingPkg of
                        Nothing -> False
                        Just pkg -> let timestamps = map fst $ map snd (V.toList (pkgMetadataRevisions pkg)) ++ map snd (V.toList (pkgTarballRevisions pkg))
                                        ultime = posixSecondsToUTCTime $ realToFrac $ entryTime entry
                                  in not . null $ filter (>= ultime) timestamps
  let isPackageJson = "package.json" `isSuffixOf` fromTarPathToPosixPath (entryTarPath entry)
  case (isPackageJson, replayAlreadyDone, packageVersionAlreadyExists) of
    (True,_,_) -> print "ignoring packagejson"
    (_,True,_) -> print "replay already done"
    (_,_,True) -> print "isrevision" -- >> doPackageRevision
    _          -> print "isupload" -- >> doPackageUpload
  return ()

createPackageTarball :: BlobStorage.BlobStorage -> PackageIdentifier -> IO PkgTarball
createPackageTarball store pkgid = do
     --  this is arguably dirty and we should use hackage-security directly, maybe?
     let pname = display (pkgName pkgid)
     let pversion = display (pkgVersion pkgid)
     let fname = display pkgid <.> "tar.gz"
     homeDir <- getHomeDirectory
     ph <- runProcess "cabal" ["fetch", "--no-dependencies", display pkgid] Nothing Nothing Nothing Nothing Nothing
     waitForProcess ph
     fileContent <- BS.readFile $ homeDir </> ".cabal/packages/hackage.haskell.org" </> pname </> pversion </> fname
     blobId <- BlobStorage.add store fileContent
     infoGz <- blobInfoFromId store blobId
     let decompressedContent = GZip.decompressNamed fname fileContent
     blobIdDecompressed <- BlobStorage.add store decompressedContent
     return  $ PkgTarball {
                              pkgTarballGz   = infoGz
                            , pkgTarballNoGz = blobIdDecompressed
                          }
readIndex :: (PackageIdentifier -> Tar.Entry -> pkg)
     -> (FilePath -> Bool) -- ^ Should this file be included?
     -> ByteString
     -> Either String [Either Tar.Entry pkg]
readIndex mkPackage includeFile indexFileContent = collect [] entries
  where
    entries = Tar.read indexFileContent
    collect es' Tar.Done        = Right es'
    collect es' (Tar.Next e es) = collect (entry e:es') es
    collect _   (Tar.Fail err)  = Left (show err)

    entry e
      | [pkgname,versionStr,_] <- splitDirectories (normalise (Tar.entryPath e))
      , Just version <- simpleParse versionStr
      , True <- includeFile (Tar.entryPath e)
      = let pkgid = PackageIdentifier (mkPackageName pkgname) version
         in Right (mkPackage pkgid e)
    entry e = Left e