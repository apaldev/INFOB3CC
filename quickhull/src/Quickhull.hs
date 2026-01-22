{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RebindableSyntax #-}
{-# LANGUAGE TypeOperators #-}

module Quickhull
  ( Point, Line, SegmentedPoints,
    quickhull,

    -- Exported for display
    initialPartition,
    partition,

    -- Exported just for testing
    propagateL, shiftHeadFlagsL, segmentedScanl1,
    propagateR, shiftHeadFlagsR, segmentedScanr1,
  ) where

import Data.Array.Accelerate
import Data.Array.Accelerate.Debug.Trace
import Data.Array.Accelerate.Smart (Acc (Acc))
import qualified Prelude as P

-- Points and lines in two-dimensional space
--
type Point = (Int, Int)
type Line = (Point, Point)

-- This algorithm will use a head-flags array to distinguish the different
-- sections of the hull (the two arrays are always the same length).
--
-- A flag value of 'True' indicates that the corresponding point is
-- definitely on the convex hull. The points after the 'True' flag until
-- the next 'True' flag correspond to the points in the same segment, and
-- where the algorithm has not yet decided whether or not those points are
-- on the convex hull.
--
type SegmentedPoints = (Vector Bool, Vector Point)

-- Core implementation
-- -------------------

-- Initialise the algorithm by first partitioning the array into two
-- segments. Locate the left-most (p₁) and right-most (p₂) points. The
-- segment descriptor then consists of the point p₁, followed by all the
-- points above the line (p₁,p₂), followed by the point p₂, and finally all
-- of the points below the line (p₁,p₂).
--
-- To make the rest of the algorithm a bit easier, the point p₁ is again
-- placed at the end of the array.
--
-- We indicate some intermediate values that you might find beneficial to
-- compute.
--
initialPartition :: Acc (Vector Point) -> Acc SegmentedPoints
initialPartition points =
  let p1, p2 :: Exp Point
      p1 =
        the
          ( fold1
              ( \a b ->
                  let (T2 ax ay) = a
                      (T2 bx by) = b
                   in if ax < bx || (ax == bx && ay <= by) then a else b
              )
              points
          )

      p2 =
        the
          ( fold1
              ( \a b ->
                  let (T2 ax ay) = a
                      (T2 bx by) = b
                   in if ax > bx || (ax == bx && ay >= by) then a else b
              )
              points
          )

      line1 = T2 p1 p2
      line2 = T2 p2 p1

      isUpper :: Acc (Vector Bool)
      isUpper = map (\p -> pointIsLeftOfLine line1 p && not (pointEq p p1) && not (pointEq p p2)) points

      isLower :: Acc (Vector Bool)
      isLower = map (\p -> pointIsLeftOfLine line2 p && not (pointEq p p1) && not (pointEq p p2)) points

      offsetUpper :: Acc (Vector Int)
      countUpper :: Acc (Scalar Int)
      T2 offsetUpper countUpper = scanl' (+) 0 (map boolToInt isUpper)

      offsetLower :: Acc (Vector Int)
      countLower :: Acc (Scalar Int)
      T2 offsetLower countLower = scanl' (+) 0 (map boolToInt isLower)

      theCountUpper = the countUpper
      theCountLower = the countLower
      theSz = 3 + theCountUpper + theCountLower

      destination :: Acc (Vector (Maybe DIM1))
      destination =
        generate
          (shape points)
          ( \i ->
              let p = points ! i
                  offU = offsetUpper ! i
                  offL = offsetLower ! i
               in if pointEq p p1
                    then lift (Just (index1 0))
                    else
                      if isUpper ! i
                        then lift (Just (index1 (1 + offU)))
                        else
                          if pointEq p p2
                            then lift (Just (index1 (1 + theCountUpper)))
                            else
                              if isLower ! i
                                then lift (Just (index1 (2 + theCountUpper + offL)))
                                else lift (Nothing :: Maybe DIM1)
          )

      newPoints :: Acc (Vector Point)
      newPoints = permute const (fill (index1 theSz) p1) (destination !) points

      -- ensure the closing point is flagged.
      headFlags :: Acc (Vector Bool)
      headFlags =
        generate
          (index1 theSz)
          ( \i ->
              let idx = unindex1 i
               in idx == 0 || idx == (1 + theCountUpper) || idx == (theSz - 1)
          )
   in T2 headFlags newPoints

pointEq :: Exp Point -> Exp Point -> Exp Bool
pointEq a b =
  let T2 ax ay = a
      T2 bx by = b
   in ax == bx && ay == by

-- The core of the algorithm processes all line segments at once in
-- data-parallel. This is similar to the previous partitioning step, except
-- now we are processing many segments at once.
--
-- For each line segment (p₁,p₂) locate the point furthest from that line
-- p₃. This point is on the convex hull. Then determine whether each point
-- p in that segment lies to the left of (p₁,p₃) or the right of (p₂,p₃).
-- These points are undecided.
--
partition :: Acc SegmentedPoints -> Acc SegmentedPoints
partition (T2 flags points) =
  let p1_for_each = propagateL flags points
      p2_for_each = propagateR flags points

      distances =
        zipWith3
          (\p1 p2 p -> abs (nonNormalizedDistance (T2 p1 p2) p))
          p1_for_each
          p2_for_each
          points

      -- Use projection onto the segment p1->p2 as a tie-breaker.
      -- maximize the projection (furthest along p1->p2).
      projections =
        zipWith3
          ( \p1 p2 p ->
              let T2 x1 y1 = p1
                  T2 x2 y2 = p2
                  T2 x y = p
                  vx = x - x1
                  vy = y - y1
                  lx = x2 - x1
                  ly = y2 - y1
               in vx * lx + vy * ly :: Exp Int
          )
          p1_for_each
          p2_for_each
          points

      indices = generate (shape points) unindex1
      -- Combine distance, projection score, and index
      dist_score_index = zipWith3 (\d s i -> T2 d (T2 s i)) distances projections indices

      argmax_scan =
        segmentedScanl1
          ( \v1 v2 ->
              let T2 d1 (T2 s1 i1) = v1
                  T2 d2 (T2 s2 i2) = v2
               in if d1 > d2 || (d1 == d2 && s1 > s2) then v1 else v2
          )
          flags
          dist_score_index

      argmax_per_segment = propagateR (shiftHeadFlagsL flags) argmax_scan
      p3_indices = map (\v -> let T2 _ (T2 _ i) = v in i) argmax_per_segment
      p3s = map (\i -> points ! index1 i) p3_indices

      is_undecided = map not flags
      is_left_p1p3 = zipWith3 (\p1 p3 p -> pointIsLeftOfLine (T2 p1 p3) p) p1_for_each p3s points
      is_left_p3p2 = zipWith3 (\p3 p2 p -> pointIsLeftOfLine (T2 p3 p2) p) p3s p2_for_each points

      mask1 =
        zipWith3
          (\u l1 l2 -> if u && l1 && not l2 then 1 else 0 :: Exp Int)
          is_undecided
          is_left_p1p3
          is_left_p3p2
      mask2 =
        zipWith3
          (\u l1 l2 -> if u && not l1 && l2 then 1 else 0 :: Exp Int)
          is_undecided
          is_left_p1p3
          is_left_p3p2

      res_scan1 = scanl' (+) 0 mask1
      res_scan2 = scanl' (+) 0 mask2
      (offset1, _) = unlift res_scan1 :: (Acc (Vector Int), Acc (Scalar Int))
      (offset2, _) = unlift res_scan2 :: (Acc (Vector Int), Acc (Scalar Int))

      local_off1 = segmentedScanl1 (+) flags mask1
      local_off2 = segmentedScanl1 (+) flags mask2

      count1_per_seg = propagateR (shiftHeadFlagsL flags) local_off1
      count2_per_seg = propagateR (shiftHeadFlagsL flags) local_off2

      local_sizes =
        zipWith5
          (\f c1 c2 p3i ix -> if f then (if p3i == ix then 1 else 2 + c1 + c2) else 0 :: Exp Int)
          flags
          count1_per_seg
          count2_per_seg
          p3_indices
          indices

      res_global = scanl' (+) 0 local_sizes
      (global_offsets, total_sum_scalar) = unlift res_global :: (Acc (Vector Int), Acc (Scalar Int))
      the_total_sz = the total_sum_scalar
      starts = propagateL flags global_offsets

      destination =
        zipWith7
          ( \f m1 m2 s lo1 lo2 c1 ->
              if f
                then
                  lift (Just (index1 s))
                else
                  if m1 == 1
                    then
                      lift (Just (index1 (s + 1 + lo1 - 1)))
                    else
                      if m2 == 1
                        then
                          lift (Just (index1 (s + 2 + c1 + lo2 - 1)))
                        else
                          lift (Nothing :: Maybe DIM1)
          )
          flags
          mask1
          mask2
          starts
          local_off1
          local_off2
          count1_per_seg

      blankPoints = fill (index1 the_total_sz) (T2 0 0)
      newPoints = permute const blankPoints (destination !) points

      p3_dest =
        zipWith4
          ( \f s c1 p3i ->
              let ix = unindex1 s
               in if f && (p3i /= ix)
                    then lift (Just (index1 (starts ! index1 ix + 1 + c1)))
                    else lift (Nothing :: Maybe DIM1)
          )
          flags
          (generate (shape flags) P.id)
          count1_per_seg
          p3_indices

      newPointsWithP3 = permute const newPoints (p3_dest !) p3s

      flagsP1 =
        permute
          const
          (fill (index1 the_total_sz) (constant False))
          ( \i ->
              if flags ! i
                then lift (Just (index1 (starts ! i)))
                else lift (Nothing :: Maybe DIM1)
          )
          (fill (shape flags) (constant True))

      finalFlags =
        permute
          const
          flagsP1
          ( \i ->
              let c1 = count1_per_seg ! i
                  p3i = p3_indices ! i
                  ix = unindex1 i
               in if flags ! i && (p3i /= ix)
                    then lift (Just (index1 (starts ! i + 1 + c1)))
                    else lift (Nothing :: Maybe DIM1)
          )
          (fill (shape flags) (constant True))
   in T2 finalFlags newPointsWithP3

-- The completed algorithm repeatedly partitions the points until there are
-- no undecided points remaining. What remains is the convex hull.
--
quickhull :: Acc (Vector Point) -> Acc (Vector Point)
quickhull points =
  let initParts = initialPartition points

      check :: Acc SegmentedPoints -> Acc (Scalar Bool)
      check (T2 flags _) = map not (Data.Array.Accelerate.and flags)

      finalState :: Acc SegmentedPoints
      finalState = awhile check partition initParts

      (T2 _ finalPoints) = finalState

      len = Data.Array.Accelerate.length finalPoints
   in -- Remove the duplicate point (p1) added at the end
      Data.Array.Accelerate.take (len - 1) finalPoints

-- Helper functions
-- ----------------

-- >>> import Data.Array.Accelerate.Interpreter
-- >>> let flags  = fromList (Z :. 9) [True,False,False,True,True,False,False,False,True]
-- >>> let values = fromList (Z :. 9) [1   ,2    ,3    ,4   ,5   ,6    ,7    ,8    ,9   ] :: Vector Int
-- >>> run $ propagateL (use flags) (use values)
--
-- should be:
-- Vector (Z :. 9) [1,1,1,4,5,5,5,5,9]
propagateL :: (Elt a) => Acc (Vector Bool) -> Acc (Vector a) -> Acc (Vector a)
propagateL flags values = segmentedScanl1 const flags values

-- >>> import Data.Array.Accelerate.Interpreter
-- >>> let flags  = fromList (Z :. 9) [True,False,False,True,True,False,False,False,True]
-- >>> let values = fromList (Z :. 9) [1   ,2    ,3    ,4   ,5   ,6    ,7    ,8    ,9   ] :: Vector Int
-- >>> run $ propagateR (use flags) (use values)
--
-- should be:
-- Vector (Z :. 9) [1,4,4,4,5,9,9,9,9]
propagateR :: (Elt a) => Acc (Vector Bool) -> Acc (Vector a) -> Acc (Vector a)
propagateR flags values = reverse (segmentedScanl1 const (reverse flags) (reverse values))

-- >>> import Data.Array.Accelerate.Interpreter
-- >>> run $ shiftHeadFlagsL (use (fromList (Z :. 6) [False,False,False,True,False,True]))
--
-- should be:
-- Vector (Z :. 6) [False,False,True,False,True,True]
shiftHeadFlagsL :: Acc (Vector Bool) -> Acc (Vector Bool)
shiftHeadFlagsL arr =
  generate
    sh
    ( \ix ->
        let i = unindex1 ix
         in if i == unindex1 sh - 1 then constant True else arr ! index1 (i + 1)
    )
  where
    sh = shape arr
-- >>> import Data.Array.Accelerate.Interpreter
-- >>> run $ shiftHeadFlagsR (use (fromList (Z :. 6) [True,False,False,True,False,False]))
--
-- should be:
-- Vector (Z :. 6) [True,True,False,False,True,False]
shiftHeadFlagsR :: Acc (Vector Bool) -> Acc (Vector Bool)
shiftHeadFlagsR arr =
  generate
    sh
    ( \ix ->
        let i = unindex1 ix
         in if i == 0 then constant True else arr ! index1 (i - 1)
    )
  where
    sh = shape arr

-- >>> import Data.Array.Accelerate.Interpreter
-- >>> let flags  = fromList (Z :. 9) [True,False,False,True,True,False,False,False,True]
-- >>> let values = fromList (Z :. 9) [1   ,2    ,3    ,4   ,5   ,6    ,7    ,8    ,9   ] :: Vector Int
-- >>> run $ segmentedScanl1 (+) (use flags) (use values)
--
-- Expected answer:
-- >>> fromList (Z :. 9) [1, 1+2, 1+2+3, 4, 5, 5+6, 5+6+7, 5+6+7+8, 9] :: Vector Int
-- Vector (Z :. 9) [1,3,6,4,5,11,18,26,9]
--
-- Mind that the interpreter evaluates scans and folds sequentially, so
-- non-associative combination functions may seem to work fine here -- only to
-- fail spectacularly when testing with a parallel backend on larger inputs. ;)
segmentedScanl1 :: (Elt a) => (Exp a -> Exp a -> Exp a) -> Acc (Vector Bool) -> Acc (Vector a) -> Acc (Vector a)
segmentedScanl1 f flags values =
  let pairs = zip flags values
      -- ScanL: If 'b' (right/current) is a flag, it resets.
      segL (T2 _ aV) (T2 bF bV) = T2 bF (if bF then bV else f aV bV)
      (_, result) = unzip (scanl1 segL pairs)
   in result

-- >>> import Data.Array.Accelerate.Interpreter
-- >>> let flags  = fromList (Z :. 9) [True,False,False,True,True,False,False,False,True]
-- >>> let values = fromList (Z :. 9) [1   ,2    ,3    ,4   ,5   ,6    ,7    ,8    ,9   ] :: Vector Int
-- >>> run $ segmentedScanr1 (+) (use flags) (use values)
--
-- Expected answer:
-- >>> fromList (Z :. 9) [1, 2+3+4, 3+4, 4, 5, 6+7+8+9, 7+8+9, 8+9, 9] :: Vector Int
-- Vector (Z :. 9) [1,9,7,4,5,30,24,17,9]
segmentedScanr1 :: (Elt a) => (Exp a -> Exp a -> Exp a) -> Acc (Vector Bool) -> Acc (Vector a) -> Acc (Vector a)
segmentedScanr1 f flags values =
  let pairs = zip flags values
      -- ScanR: If 'a' (left/current) is a flag, it resets.
      segR (T2 aF aV) (T2 bF bV) = T2 aF (if aF then aV else f aV bV)
      (_, result) = unzip (scanr1 segR pairs)
   in result

-- Given utility functions
-- -----------------------

pointIsLeftOfLine :: Exp Line -> Exp Point -> Exp Bool
pointIsLeftOfLine (T2 (T2 x1 y1) (T2 x2 y2)) (T2 x y) = nx * x + ny * y > c
  where
    nx = y1 - y2
    ny = x2 - x1
    c = nx * x1 + ny * y1

nonNormalizedDistance :: Exp Line -> Exp Point -> Exp Int
nonNormalizedDistance (T2 (T2 x1 y1) (T2 x2 y2)) (T2 x y) = nx * x + ny * y - c
  where
    nx = y1 - y2
    ny = x2 - x1
    c = nx * x1 + ny * y1

segmented :: (Elt a) => (Exp a -> Exp a -> Exp a) -> Exp (Bool, a) -> Exp (Bool, a) -> Exp (Bool, a)
segmented f (T2 aF aV) (T2 bF bV) = T2 (aF || bF) (if bF then bV else f aV bV)

-- | Read a file (such as "inputs/1.dat") and return a vector of points,
-- suitable as input to 'quickhull' or 'initialPartition'. Not to be used in
-- your quickhull algorithm, but can be used to test your functions in ghci.
readInputFile :: P.FilePath -> P.IO (Vector Point)
readInputFile filename =
  (\l -> fromList (Z :. P.length l) l)
    P.. P.map (\l -> let ws = P.words l in (P.read (ws P.!! 0), P.read (ws P.!! 1)))
    P.. P.lines
    P.<$> P.readFile filename