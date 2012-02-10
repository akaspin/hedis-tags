{-# LANGUAGE OverloadedStrings #-} 

-- | Hedis tags helper.

module Database.Redis.Tags (
    -- * Tagging
    markTags,
    purgeTags,
    nestTags,
    -- * Maintenance
    reconsileTags
) where

import qualified Data.ByteString as B
import qualified Database.Redis as R
import Data.Either (rights)

import Control.Monad (void, filterM)

-- | Mark keys with tags. Keys may be absent. All tags named in next manner:
--
-- > tag-prefix:tag:tag-signature
-- 
--   Tags stored in Redis as sets with no expiration. Tags not related to 
--   each other.
--
--   /Time complexity/ @O(K+T)@ where @K@ and @T@ is number of keys and tags.
markTags :: 
       [B.ByteString]   -- ^ Keys.
    -> B.ByteString     -- ^ Prefix for tags. 
    -> [B.ByteString]   -- ^ Tags. To make list of nested tags use 'nestTags'.
    -> R.Redis ()
markTags [] _ _ = return ()
markTags _ _ [] = return ()
markTags keys pref tags =
    let pt = map (tagName pref) tags in 
    void $ mapM (`R.sadd` keys) pt

-- | Purge tagged keys and tags. 
--
--   Because the tags are not related to each other, if key tagged with more 
--   than one tags, remember the following. After removal of one of the tags, 
--   may remain orphans. To avoid this, purge all needed tags or use 
--   'reconcileTags' for stripping.
--
--   /Time complexity/ @~O(T+2K)@ where @T@ is number tags and @K@ is number 
--   of tagged keys.
purgeTags :: 
       B.ByteString    -- ^ Prefix for tags.  
    -> [B.ByteString]  -- ^ Tags. To make list of nested tags use 'nestTags'.
    -> R.Redis ()
purgeTags _ [] = return ()
purgeTags pref tags = do
    let pt = map (tagName pref) tags
    a <- R.sunion pt
    let keys = head $ rights [a]
    void $ R.del pt 
    void $ R.del keys 

-- | Helper for create list of nested tags.
--
-- > nestTags ["one", "two", "three"]
-- > ["one", "one:two", "one:two:three"]
nestTags :: [B.ByteString] -> [B.ByteString]
nestTags = scanl1 (\a b -> B.append a $ B.append ":" b)

-- | Reconcile all tags with given prefix.
--
--   * Remove noexistent keys from tags.
--   
--   * Remove empty tags.
--   
--   This operation take huge time complexity. Use it only for maintenance. 
reconsileTags :: 
       B.ByteString   -- ^ Tags prefix. 
    -> R.Redis ()
reconsileTags pref = do
    allTags <- R.keys $ tagName pref "*"
    needRem <- filterM reconsileTag $ head $ rights [allTags]
    void $ R.del needRem
  where
    reconsileTag t = do
        keys <- R.smembers t
        let keys' = head $ rights [keys]
        needRem <- filterM checkKey keys'
        if needRem == keys'
            then return True
            else do 
                void $ R.srem t needRem
                return False
    checkKey k = do
        ex <- R.exists k
        return $ case ex of
            Right False -> True
            _ -> False
    
-----------------------------------------------------------------------------
-- Internal
-----------------------------------------------------------------------------

-- | Make tag name with prefix
tagName :: B.ByteString -> B.ByteString -> B.ByteString
tagName pref = (pref `B.append` ":tag:" `B.append`)