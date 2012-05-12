{-# LANGUAGE OverloadedStrings, RankNTypes #-} 
-- | Tags tests

module Database.Redis.Tags.Test.Tags (tests) where

import Test.Framework (testGroup, mutuallyExclusive, Test)
import Test.Framework.Providers.HUnit (testCase)
import Test.HUnit (Assertion, (@=?))

import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Control.Exception.Lifted (bracket_)

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8

import qualified Database.Redis as R
import qualified Database.Redis.Tags as RT
import Data.List (sort)

tests :: Test
tests = mutuallyExclusive $ testGroup "Tags" [
    testCase "Nested tags" caseNestTags,
    testCase "Mark" caseMark,
    testCase "Purge" casePurge,
    testCase "Reconcile" caseReconsile
    ]

caseNestTags :: Assertion
caseNestTags = do
    let expect = ["one", "one:two", "one:two:three"]
    expect @=? RT.nestTags ["one", "two", "three"]

caseMark :: Assertion
caseMark = bracket_
    setup
    teardown
    $ runInRedis $ do
        let expected = appendScrap [1..9]
        RT.markTags expected
                    allPrefix
                    (scrap [1..3])
        let tags = appendPrefix . map (":tag:" `B.append`) $ scrap [1..3]
        res <- mapM R.smembers tags
        mapM_ (liftIO . 
               (Right expected @=?) . 
               fmap sort) res

casePurge :: Assertion
casePurge = bracket_
    setup
    teardown
    $ runInRedis $ do
        let expected = appendScrap [1..9]
        let tags = appendPrefix . map (":tag:" `B.append`) $ scrap [1..3]
        RT.markTags expected
                    allPrefix
                    (scrap [1..3])
        RT.purgeTags allPrefix (scrap [1..3])
        mapM_ (\k -> do 
            ex <- R.exists k
            liftIO $ ex @=? Right False
            ) expected
        mapM_ (\k -> do 
            ex <- R.exists k
            liftIO $ ex @=? Right False
            ) tags

caseReconsile :: Assertion
caseReconsile = bracket_
    setup
    teardown
    $ runInRedis $ do
        let expected = scrap9
        let tags = appendPrefix . map (":tag:" `B.append`) $ scrap [1..3]
        RT.markTags expected
                    allPrefix
                    (scrap [1..3])
        void $ R.del $ appendScrap [1..8]
        RT.reconsileTags allPrefix
        t <- R.keys $ allPrefix `B.append` ":tag:*"
        k <- R.sunion tags
        liftIO $ Right tags @=? t
        liftIO $ Right ["redistags9"] @=? k
        void $ R.del scrap9
        RT.reconsileTags allPrefix
        t' <- R.keys $ allPrefix `B.append` ":tag:*"
        k' <- R.sunion tags
        liftIO $ Right [] @=? t'
        liftIO $ Right [] @=? k'
  where
    scrap9 = appendScrap [1..9]
        
        
runInRedis :: forall b. R.Redis b -> IO b
runInRedis a = do
    conn <- R.connect R.defaultConnectInfo
    R.runRedis conn a
    
-- | Populate keys
setup :: IO ()
setup = runInRedis $ void $ R.mset $ zip 
    (appendPrefix . scrap $ [1..100]) 
    (appendPrefix . scrap $ [1..100])

-- | Purge all keys with 'allPrefix'
teardown :: IO ()
teardown = runInRedis $ do
    a <- R.keys $ allPrefix `B.append` "*"
    _ <- either undefined R.del a
    return ()

-- | Scrap bytestring list
scrap :: [Int] -> [B.ByteString]
scrap = map (B8.pack . show)

appendPrefix :: [B.ByteString] -> [B.ByteString]
appendPrefix = map (allPrefix `B.append`)

appendScrap :: [Int] -> [B8.ByteString]
appendScrap = appendPrefix . scrap

allPrefix :: B.ByteString
allPrefix = "redistags"

