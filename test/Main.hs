
module Main (main) where

import Test.Framework (defaultMain)

import qualified Database.Redis.Tags.Test.Tags

main :: IO ()
main = defaultMain [
    Database.Redis.Tags.Test.Tags.tests
    ]

