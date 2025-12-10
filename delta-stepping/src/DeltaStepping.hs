{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}

--
-- INFOB3CC Concurrency
-- Practical 2: Single Source Shortest Path
--
--    Δ-stepping: A parallelisable shortest path algorithm
--    https://www.sciencedirect.com/science/article/pii/S0196677403000762
--
-- https://ics.uu.nl/docs/vakken/b3cc/assessment.html
--
-- https://cs.iupui.edu/~fgsong/LearnHPC/sssp/deltaStep.html
--

module DeltaStepping
  ( Graph,
    Node,
    Distance,
    deltaStepping,
  )
where

import Control.Concurrent
import Control.Concurrent.MVar
import Control.Monad
import Data.Bits
import Data.Graph.Inductive (Gr)
import qualified Data.Graph.Inductive as G
import Data.IORef
import Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as Map
import Data.IntSet (IntSet)
import qualified Data.IntSet as Set
import Data.List (foldl')
import qualified Data.Vector as Vec
import qualified Data.Vector.Mutable as V
import Data.Vector.Storable (Vector)
import qualified Data.Vector.Storable as S (unsafeFreeze)
import qualified Data.Vector.Storable.Mutable as S
import Data.Word
import Foreign.Ptr
import Foreign.Storable
import Sample
import Text.Printf
import Utils

type Graph = Gr String Distance -- Graphs have nodes labelled with Strings and edges labelled with their distance

type Node = Int -- Nodes (vertices) in the graph are integers in the range [0..]

type Distance = Float -- Distances between nodes are (positive) floating point values

-- | Find the length of the shortest path from the given node to all other nodes
-- in the graph. If the destination is not reachable from the starting node the
-- distance is 'Infinity'.
--
-- Nodes must be numbered [0..]
--
-- Negative edge weights are not supported.
--
-- NOTE: The type of the 'deltaStepping' function should not change (since that
-- is what the test suite expects), but you are free to change the types of all
-- other functions and data structures in this module as you require.
deltaStepping ::
  Bool -> -- Whether to print intermediate states to the console, for debugging purposes
  Graph -> -- graph to analyse
  Distance -> -- delta (step width, bucket width)
  Node -> -- index of the starting node
  IO (Vector Distance)
deltaStepping verbose graph delta source = do
  threadCount <- getNumCapabilities

  (buckets, distances) <- initialise graph delta source
  printVerbose verbose "initialse" graph delta buckets distances

  let loop = do
        done <- allBucketsEmpty buckets
        if done
          then return ()
          else do
            printVerbose verbose "result" graph delta buckets distances
            step verbose threadCount graph delta buckets distances
            loop
  loop

  printVerbose verbose "result" graph delta buckets distances
  S.unsafeFreeze distances

-- Initialise algorithm state
--
initialise graph delta source = do
  let nodes = G.nodes graph
      maxNode = if null nodes then 0 else maximum nodes
      nodeCount = maxNode + 1

  distances <- S.replicate nodeCount infinity
  S.write distances source 0

  let numBuckets = 128
  bucketArray <- V.replicate numBuckets Set.empty

  V.write bucketArray 0 (Set.singleton source)

  firstBucket <- newIORef 0

  return (Buckets firstBucket bucketArray, distances)

-- Take a single step of the algorithm.
-- That is, one iteration of the outer while loop.
--
step ::
  Bool ->
  Int ->
  Graph ->
  Distance ->
  Buckets ->
  TentativeDistances ->
  IO ()
step verbose threadCount graph delta buckets@Buckets {..} distances = do
  i <- findNextBucket buckets
  writeIORef firstBucket i

  let processLightEdges removedNodes = do
        let idx = i `rem` V.length bucketArray
        nodes <- V.read bucketArray idx

        if Set.null nodes
          then return removedNodes
          else do
            V.write bucketArray idx Set.empty

            printVerbose verbose "inner step" graph delta buckets distances

            reqs <- findRequests threadCount (\w -> w <= delta) graph nodes distances

            relaxRequests threadCount buckets distances delta reqs

            processLightEdges (Set.union removedNodes nodes)

  nodesProcessed <- processLightEdges Set.empty

  unless (Set.null nodesProcessed) $ do
    reqs <- findRequests threadCount (\w -> w > delta) graph nodesProcessed distances
    relaxRequests threadCount buckets distances delta reqs

  writeIORef firstBucket (i + 1)

-- Once all buckets are empty, the tentative distances are finalised and the
-- algorithm terminates.
--
allBucketsEmpty :: Buckets -> IO Bool
allBucketsEmpty Buckets {..} = check 0
  where
    len = V.length bucketArray
    check i
      | i >= len = return True
      | otherwise = do
          bucket <- V.read bucketArray i
          if Set.null bucket
            then check (i + 1)
            else return False

-- Return the index of the first non-empty bucket. Assumes that there is at
-- least one non-empty bucket remaining.
--
findNextBucket :: Buckets -> IO Int
findNextBucket Buckets {..} = do
  start <- readIORef firstBucket
  let len = V.length bucketArray
      go i = do
        let idx = i `rem` len
        b <- V.read bucketArray idx
        if Set.null b
          then go (i + 1)
          else return i
  go start

splitWork :: Int -> Int -> [a] -> [[a]]
splitWork numChunks totalSize xs = go xs numChunks totalSize
  where
    go _ 0 _ = []
    go ys k remaining =
      let chunkSize = (remaining + k - 1) `div` k
          (chunk, rest) = splitAt chunkSize ys
       in chunk : go rest (k - 1) (remaining - chunkSize)

minWorkThreshold :: Int
minWorkThreshold = 500

mergeTree :: [IntMap Distance] -> IntMap Distance
mergeTree = foldl' (Map.unionWith min) Map.empty

-- Create requests of (node, distance) pairs that fulfil the given predicate
--
findRequests ::
  Int ->
  (Distance -> Bool) ->
  Graph ->
  IntSet ->
  TentativeDistances ->
  IO (IntMap Distance)
findRequests threadCount predicate graph nodes distances = do
  let nodeList = Set.toList nodes
      workSize = Set.size nodes
      useParallel = threadCount > 1 && workSize > minWorkThreshold

  if not useParallel
    then findRequestsSeq nodeList
    else findRequestsPar nodeList workSize threadCount
  where
    findRequestsSeq ns = do
      currentDists <- mapM (S.read distances) ns
      let requests = concat $ zipWith generateEdges ns currentDists
      return $ Map.fromListWith min requests

    findRequestsPar ns totalWork tc = do
      resultVars <- Vec.replicateM tc newEmptyMVar
      let !chunks = Vec.fromList $ splitWork tc totalWork ns

      forkThreads tc $ \tid -> do
        let !myNodes = chunks Vec.! tid
        currentDists <- mapM (S.read distances) myNodes
        let !requests = concat $ zipWith generateEdges myNodes currentDists
            !localMap = Map.fromListWith min requests
        putMVar (resultVars Vec.! tid) localMap

      maps <- mapM takeMVar (Vec.toList resultVars)

      return $! mergeTree maps

    generateEdges u distU =
      [ (v, distU + w)
        | (_, v, w) <- G.out graph u,
          predicate w
      ]

-- Execute requests for each of the given (node, distance) pairs
--
relaxRequests ::
  Int ->
  Buckets ->
  TentativeDistances ->
  Distance ->
  IntMap Distance ->
  IO ()
relaxRequests threadCount buckets distances delta reqs = do
  let reqList = Map.toList reqs
      workSize = Map.size reqs
      useParallel = threadCount > 1 && workSize > minWorkThreshold

  if not useParallel
    then mapM_ (relax buckets distances delta) reqList
    else do
      let !chunks = Vec.fromList $ splitWork threadCount workSize reqList

      forkThreads threadCount $ \tid -> do
        let !myReqs = chunks Vec.! tid
        mapM_ (relax buckets distances delta) myReqs

-- Execute a single relaxation, moving the given node to the appropriate bucket
-- as necessary
--
relax ::
  Buckets ->
  TentativeDistances ->
  Distance ->
  (Node, Distance) -> -- (w, x) in the paper
  IO ()
relax Buckets {..} distances delta (node, newDistance) = do
  currentDist <- S.read distances node

  when (newDistance < currentDist) $ do
    previousDistance <- atomicModifyIOVectorFloat distances node $ \oldDist ->
      if newDistance < oldDist
        then (newDistance, oldDist)
        else (oldDist, oldDist)

    when (newDistance < previousDistance) $ do
      let len = V.length bucketArray

      unless (isInfinite previousDistance) $ do
        let oldBucketIdx = floor (previousDistance / delta) `rem` len
        _ <- atomicModifyIOVector bucketArray oldBucketIdx $ \s -> (Set.delete node s, ())
        return ()

      let newBucketIdx = floor (newDistance / delta) `rem` len
      _ <- atomicModifyIOVector bucketArray newBucketIdx $ \s -> (Set.insert node s, ())
      return ()

-- -----------------------------------------------------------------------------
-- Starting framework
-- -----------------------------------------------------------------------------
--
-- Here are a collection of (data)types and utility functions that you can use.
-- You are free to change these as necessary.
--

type TentativeDistances = S.IOVector Distance

data Buckets = Buckets
  { firstBucket :: {-# UNPACK #-} !(IORef Int), -- real index of the first bucket (j)
    bucketArray :: {-# UNPACK #-} !(V.IOVector IntSet) -- cyclic array of buckets
  }

-- The initial tentative distance, or the distance to unreachable nodes
--
infinity :: Distance
infinity = 1 / 0

-- Forks 'n' threads. Waits until those threads have finished. Each thread
-- runs the supplied function given its thread ID in the range [0..n).
--
forkThreads :: Int -> (Int -> IO ()) -> IO ()
forkThreads n action = do
  -- Fork the threads and create a list of the MVars which per thread tell
  -- whether the action has finished.
  finishVars <- mapM work [0 .. n - 1]
  -- Once all the worker threads have been launched, now wait for them all to
  -- finish by blocking on their signal MVars.
  mapM_ takeMVar finishVars
  where
    -- Create a new empty MVar that is shared between the main (spawning) thread
    -- and the worker (child) thread. The main thread returns immediately after
    -- spawning the worker thread. Once the child thread has finished executing
    -- the given action, it fills in the MVar to signal to the calling thread
    -- that it has completed.
    --
    work :: Int -> IO (MVar ())
    work index = do
      done <- newEmptyMVar
      _ <- forkOn index (action index >> putMVar done ()) -- pin the worker to a given CPU core
      return done

printVerbose :: Bool -> String -> Graph -> Distance -> Buckets -> TentativeDistances -> IO ()
printVerbose verbose title graph delta buckets distances = when verbose $ do
  putStrLn $ "# " ++ title
  printCurrentState graph distances
  printBuckets graph delta buckets distances
  putStrLn "Press enter to continue"
  _ <- getLine
  return ()

-- Print the current state of the algorithm (tentative distance to all nodes)
--
printCurrentState ::
  Graph ->
  TentativeDistances ->
  IO ()
printCurrentState graph distances = do
  printf "  Node  |  Label  |  Distance\n"
  printf "--------+---------+------------\n"
  forM_ (G.labNodes graph) $ \(v, l) -> do
    x <- S.read distances v
    if isInfinite x
      then printf "  %4d  |  %5v  |  -\n" v l
      else printf "  %4d  |  %5v  |  %f\n" v l x
  --
  printf "\n"

printBuckets ::
  Graph ->
  Distance ->
  Buckets ->
  TentativeDistances ->
  IO ()
printBuckets graph delta Buckets {..} distances = do
  first <- readIORef firstBucket
  mapM_
    ( \idx -> do
        let idx' = first + idx
        printf "Bucket %d: [%f, %f)\n" idx' (fromIntegral idx' * delta) ((fromIntegral idx' + 1) * delta)
        b <- V.read bucketArray (idx' `rem` V.length bucketArray)
        printBucket graph b distances
    )
    [0 .. V.length bucketArray - 1]

-- Print the current bucket
--
printCurrentBucket ::
  Graph ->
  Distance ->
  Buckets ->
  TentativeDistances ->
  IO ()
printCurrentBucket graph delta Buckets {..} distances = do
  j <- readIORef firstBucket
  b <- V.read bucketArray (j `rem` V.length bucketArray)
  printf "Bucket %d: [%f, %f)\n" j (fromIntegral j * delta) (fromIntegral (j + 1) * delta)
  printBucket graph b distances

-- Print a given bucket
--
printBucket ::
  Graph ->
  IntSet ->
  TentativeDistances ->
  IO ()
printBucket graph bucket distances = do
  printf "  Node  |  Label  |  Distance\n"
  printf "--------+---------+-----------\n"
  forM_ (Set.toAscList bucket) $ \v -> do
    let ml = G.lab graph v
    x <- S.read distances v
    case ml of
      Nothing -> printf "  %4d  |   -   |  %f\n" v x
      Just l -> printf "  %4d  |  %5v  |  %f\n" v l x
  --
  printf "\n"
