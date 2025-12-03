--
-- INFOB3CC Concurrency
-- Practical 1: IBAN calculator
--
-- http://ics.uu.nl/docs/vakken/b3cc/assessment.html
--
module IBAN (

  Mode(..), Config(..),
  count, list, search

) where

import Control.Concurrent
import Crypto.Hash.SHA1
import Data.Atomics                                       ( readForCAS, casIORef, peekTicket )
import Data.IORef
import Data.List                                         
import Data.Word
import Data.Maybe                                        
import System.Environment
import System.IO
import Data.ByteString.Char8                              ( ByteString )
import qualified Data.ByteString                          as B
import qualified Data.ByteString.Char8                    as B8
import Control.Monad (forM_, replicateM_, unless, when)


-- -----------------------------------------------------------------------------
-- 0. m-test
-- -----------------------------------------------------------------------------

-- Perform the m-test on 'number'. Use `div` and `mod` to extract digits from
-- the number; do not use `show`, as it is too slow.
mtest :: Int -> Int -> Bool
mtest m number =
  let digits n
        | n == 0    = []
        | otherwise = (n `mod` 10) : digits (n `div` 10)
      weighted = zipWith (*) (digits number) [1..]
  in sum weighted `mod` m == 0

-- -----------------------------------------------------------------------------
-- 1. Counting mode (3pt) [FINISHED]
-- -----------------------------------------------------------------------------

count :: Config -> IO Int
count config = do
  counterRef <- newIORef 0
  
  let totalRange = cfgUpper config - cfgLower config
      numThreads = cfgThreads config
      chunkSize = totalRange `div` numThreads
      remainder = totalRange `mod` numThreads
      
      getRangeForThread tid =
        let start = cfgLower config + tid * chunkSize + min tid remainder
            end   = cfgLower config + (tid + 1) * chunkSize + min (tid + 1) remainder
        in (start, end)
  
  forkThreads numThreads $ \tid -> do
    let (lo, hi) = getRangeForThread tid
        localCount = length [x | x <- [lo..hi - 1], mtest (cfgModulus config) x]
    
    evaluate localCount
    
    let casLoop = do
          ticket <- readForCAS counterRef
          let oldVal = peekTicket ticket
              newVal = oldVal + localCount
          (success, _) <- casIORef counterRef ticket newVal
          if success
            then return ()
            else casLoop
    
    casLoop
  
  readIORef counterRef

-- -----------------------------------------------------------------------------
-- 2. List mode (3pt)
-- -----------------------------------------------------------------------------

list :: Handle -> Config -> IO ()
list handle config = do
  seqNumVar <- newMVar (1 :: Int)
  
  let totalRange = cfgUpper config - cfgLower config
      numThreads = cfgThreads config
      chunkSize = totalRange `div` numThreads
      remainder = totalRange `mod` numThreads
      
      getRangeForThread tid =
        let start = cfgLower config + tid * chunkSize + min tid remainder
            end   = cfgLower config + (tid + 1) * chunkSize + min (tid + 1) remainder
        in (start, end)
  
  forkThreads numThreads $ \tid -> do
    let (lo, hi) = getRangeForThread tid
        validNums = [x | x <- [lo..hi - 1], mtest (cfgModulus config) x]
    
    forM_ validNums $ \num -> do
      modifyMVar_ seqNumVar $ \seqNum -> do
        hPutStrLn handle (show seqNum ++ " " ++ show num)
        return (seqNum + 1)

-- -----------------------------------------------------------------------------
-- 3. Search mode (4pt)
-- -----------------------------------------------------------------------------

search :: Config -> ByteString -> IO (Maybe Int)
search config query = do
  resultVar      <- newEmptyMVar
  workChan       <- newChan :: IO (Chan (Maybe (Int, Int)))
  activeTasksRef <- newIORef 0
  doneRef        <- newIORef False

  let cutoff      = 5000
      threadCount = cfgThreads config
      modulus     = cfgModulus config

      publishResult :: Maybe Int -> IO ()
      publishResult res = do
        alreadyDone <- atomicModifyIORef' doneRef $ \done ->
          if done then (done, True) else (True, False)
        unless alreadyDone $ do
          putMVar resultVar res
          replicateM_ threadCount (writeChan workChan Nothing)

      finishTask :: IO ()
      finishTask = do
        remaining <- atomicModifyIORef' activeTasksRef $ \c -> let n = c - 1 in (n, n)
        when (remaining == 0) $ publishResult Nothing

      enqueueWork :: [(Int, Int)] -> IO ()
      enqueueWork ranges = do
        let valid = filter (uncurry (<)) ranges
            added = length valid
        unless (added == 0) $ do
          atomicModifyIORef' activeTasksRef (\c -> (c + added, ()))
          mapM_ (writeChan workChan . Just) valid

      findMatch :: Int -> Int -> Maybe Int
      findMatch lo hi = go lo
        where
          go n
            | n >= hi = Nothing
            | mtest modulus n && checkHash query (show n) = Just n
            | otherwise = go (n + 1)

      splitRange :: Int -> Int -> [(Int, Int)]
      splitRange lo hi =
        let rangeSize = hi - lo
            quarter   = max 1 (rangeSize `div` 4)
            mid1      = min hi (lo + quarter)
            mid2      = min hi (lo + 2 * quarter)
            mid3      = min hi (lo + 3 * quarter)
        in [(lo, mid1), (mid1, mid2), (mid2, mid3), (mid3, hi)]

      worker :: IO ()
      worker = do
        task <- readChan workChan
        case task of
          Nothing -> return ()
          Just (lo, hi) -> do
            done <- readIORef doneRef
            if done
              then finishTask >> worker
              else if hi - lo <= cutoff
                then do
                  case findMatch lo hi of
                    Just result -> publishResult (Just result)
                    Nothing -> finishTask >> worker
                else do
                  enqueueWork (splitRange lo hi)
                  finishTask
                  worker

  enqueueWork [(cfgLower config, cfgUpper config)]
  forkThreads threadCount (const worker)
  takeMVar resultVar

-- -----------------------------------------------------------------------------
-- Starting framework
-- -----------------------------------------------------------------------------

data Mode = Count | List | Search ByteString
  deriving Show

data Config = Config
  { cfgLower   :: !Int
  , cfgUpper   :: !Int
  , cfgModulus :: !Int
  , cfgThreads :: !Int
  }
  deriving Show

-- Evaluates a term, before continuing with the next IO operation.
--
evaluate :: a -> IO ()
evaluate x = x `seq` return ()

-- Forks 'n' threads. Waits until those threads have finished. Each thread
-- runs the supplied function given its thread ID in the range [0..n).
--
forkThreads :: Int -> (Int -> IO ()) -> IO ()
forkThreads n work = do
  -- Fork the threads and create a list of the MVars which
  -- per thread tell whether the work has finished.
  finishVars <- mapM work' [0 .. n - 1]
  -- Wait on all MVars
  mapM_ takeMVar finishVars
  where
    work' :: Int -> IO (MVar ())
    work' index = do
      var <- newEmptyMVar
      _   <- forkOn index (work index >> putMVar var ())
      return var

-- Checks whether 'value' has the expected hash.
--
checkHash :: ByteString -> String -> Bool
checkHash expected value = expected == hash (B8.pack value)
