module MapReduce where

import Data.Ord
import Data.List
import Test.HUnit

---------------------------------Sección 1---------Diccionario ---------------------------
type Dict k v = [(k,v)]

-- Ejercicio 1

belongs :: Eq k => k -> Dict k v -> Bool
belongs key = foldr keyDetector False
          where keyDetector par r =  r || fst par == key

(?) :: Eq k => Dict k v -> k -> Bool
(?) = flip belongs 

-- Ejercicio 2
get :: Eq k => k -> Dict k v -> v
get k d = snd $ head $ filter (keyDetector k) d
        where keyDetector k (key, val) = key == k 

(!) :: Eq k => Dict k v -> k -> v
(!) = flip get

-- Ejercicio 3
insertWith :: Eq k => (v -> v -> v) -> k -> v -> Dict k v -> Dict k v
insertWith f key val dict = if dict ? key then modifyExistingValue f key val dict else dict ++ [(key, val)]


modifyExistingValue f key val = foldr inserter [] 
                  where inserter (k, v) dict = if key == k then (k, (f v val)):dict else (k, v):dict


-- Ejercicio 4
groupByKey :: Eq k => [(k,v)] -> Dict k [v]
groupByKey d = reverse $ foldr grouper [] d
                  where grouper (k, v) dict = insertWith (++) k [v] dict

-- Ejercicio 5
unionWith :: Eq k => (v -> v -> v) -> Dict k v -> Dict k v -> Dict k v
unionWith f = foldr g
                      where g (k, v) = insertWith f k v

------------------------------Sección 2--------------MapReduce---------------------------

type Mapper a k v = a -> [(k,v)]
type Reducer k v b = (k, [v]) -> [b]

-- Ejercicio 6
distributionProcess :: Int -> [a] -> [[a]]
distributionProcess n = foldr repartir (replicate n [])
                          where repartir x rep = tail rep ++ [x:(head rep)]
-- Ejercicio 7
mapperProcess :: Eq k => Mapper a k v -> [a] -> Dict k [v]
mapperProcess m l = reverse $ groupByKey $ foldr ((++).m) [] l

-- Ejercicio 8
combinerProcess :: (Eq k, Ord k) => [Dict k [v]] -> Dict k [v]
combinerProcess l = sortBy (\(a,_) (b,_) -> compare a b) $ foldr unidor [] l
                    where unidor d re = unionWith (++) d re

-- Ejercicio 9
reducerProcess :: Reducer k v b -> Dict k [v] -> [b]
reducerProcess reducer = foldr ((++).reducer) []

-- Ejercicio 10
mapReduce :: (Eq k, Ord k) => Mapper a k v -> Reducer k v b -> [a] -> [b]
mapReduce m r l =  reducerProcess r $ combinerProcess $ mapperApplier m l

mapperApplier m l = foldr g [] (distributionProcess 100 l)
                    where g x r = (mapperProcess m x) : r 

--Funciones de prueba --

--Restos módulo 5

mapperRestos :: Mapper Int Int Int
mapperRestos n = [(n `mod` 5, n)]

reducerRestos :: Reducer Int Int (Int, Int)
reducerRestos (r, ns) = [(r, length ns)]

restosMod5 :: [Int] -> Dict Int Int
restosMod5 = mapReduce mapperRestos reducerRestos

--Clasificación de palabras
palabras :: [[(Int, [[Char]])]]
palabras = [[(1,["Hola","Chau"]),(2,["Perro","Gato"])],[(2,["Jirafa"])],[(3,["Casa"]),(4,["Tren", "Auto"]), (1, ["Saludos"])],[(2, ["Perro"]), (4, ["Barco"])]]

-- Monumentos por país

mapperMPP :: Mapper (Structure, Dict String String) String ()
mapperMPP (Monument, metadata) = let country = metadata ! "country"
                                 in [(country, ())]
mapperMPP _                  = []

reducerMPP :: Reducer String () (String, Int)
reducerMPP (country, units) = [(country, length units)]

monumentosPorPais :: [(Structure, Dict String String)] -> [(String, Int)]
monumentosPorPais = mapReduce mapperMPP reducerMPP

-- Monumentos top

mapperVPM :: Mapper String String ()
mapperVPM m = [(m, ())]

reducerVPM :: Reducer String () (String, Int)
reducerVPM (monument, units) = [(monument, length units)]

visitasPorMonumento :: [String] -> [(String, Int)]
visitasPorMonumento = mapReduce mapperVPM reducerVPM

mapperOPV :: Mapper (String, Int) Int String
mapperOPV (monument, visitCount) = [(-visitCount, monument)]

-- Acá se utiliza el orden por visitCount que provee mapReduce.
-- Sencillamente se descarta el número para devolver la lista
-- de monumentos ordenada por visitas.
reducerOPV :: Reducer Int String String
reducerOPV = snd

ordenarPorVisitas :: [(String, Int)] -> [String]
ordenarPorVisitas = mapReduce mapperOPV reducerOPV

monumentosTop :: [String] -> [String]
monumentosTop = ordenarPorVisitas.visitasPorMonumento
                

-- ------------------------ Ejemplo de datos para pruebas ----------------------
data Structure = Street | City | Monument deriving Show

items :: [(Structure, Dict String String)]
items = [
    (Monument, [
      ("name","Obelisco"),
      ("latlong","-36.6033,-57.3817"),
      ("country", "Argentina")]),
    (Street, [
      ("name","Int. Güiraldes"),
      ("latlong","-34.5454,-58.4386"),
      ("country", "Argentina")]),
    (Monument, [
      ("name", "San Martín"),
      ("country", "Argentina"),
      ("latlong", "-34.6033,-58.3817")]),
    (City, [
      ("name", "Paris"),
      ("country", "SegundoFrancia"),
      ("latlong", "-24.6033,-18.3817")]),
    (Monument, [
      ("name", "Bagdad Bridge"),
      ("country", "Irak"),
      ("new_field", "new"),
      ("latlong", "-11.6033,-12.3817")]),
    (Monument, [
      ("name", "Torre Eiffel"),
      ("country", "SegundoFrancia"),
      ("latlong", "-24.6033,-18.3817")])
    ]

------------------------------------------------

taylorAlbums :: Dict String Int
taylorAlbums = [("Taylor Swift", 2006), ("Fearless", 2008), ("Speak Now", 2010), ("Red", 2012), ("1989", 2014), 
                ("Reputation", 2017), ("Lover", 2019), ("Folklore", 2020), ("Evermore", 2020), ("Midnights", 2022)]

vaultAlbums = [("Fearless", ["You All Over Me", "Mr. Perfectly Fine", "That's When", "We Were Happy", "Don't You", "Bye Bye Baby" ]), 
               ("Speak Now", ["Electric Touch", "When Emma Falls in Love", "Foolish One", "Castles Crumbling", "Timeless", "I Can See You"]), 
               ("Red", ["Nothing New", "Run", "The Very First Night", "Forever Winter", "Babe", "Better Man"]), 
               ("1989", ["TBA 1", "TBA 2"])]

------------------------------------------------

--Ejecución de los tests
main :: IO Counts
main = do runTestTT allTests

allTests = test [
  "ejercicio1" ~: testsEj1,
  "ejercicio2" ~: testsEj2,
  "ejercicio3" ~: testsEj3,
  "ejercicio4" ~: testsEj4,
  "ejercicio5" ~: testsEj5,
  "ejercicio6" ~: testsEj6,
  "ejercicio7" ~: testsEj7,
  "ejercicio8" ~: testsEj8,
  "ejercicio8" ~: testsEj9,
  "ejercicio8" ~: testsEj10
  ]
  
testsEj1 = test [
  ([("calle",[3]),("ciudad",[2,1])] ? "ciudad")  ~=? True,
  ([("calle",[3]),("ciudad",[2,1])] ? "perro")  ~=? False,
  
  --Agregar sus propios tests.

  belongs "Evermore" taylorAlbums ~=? True,
  belongs "Reputation" vaultAlbums ~=? False,
  taylorAlbums ? "Karma" ~=? False,
  vaultAlbums ? "Fearless" ~=? True
  ]

testsEj2 = test [
  [("calle","San Blas"),("ciudad","Hurlingham")] ! "ciudad" ~=? "Hurlingham", 
  
  --Agregar sus propios tests.

  get "Speak Now" taylorAlbums ~=? 2010,
  (get "Red" vaultAlbums) !! 4 ~=? "Babe",
  (taylorAlbums ! "1989" == 1989) ~=? False
  ]

testsEj3 = test [

  (insertWith (++) 1 [99] [(1, [1]), (2, [2])]) ~=? [(1,[1,99]),(2,[2])],
  (insertWith (++) 3 [99] [(1 ,[1]), (2, [2])]) ~=? [(1,[1]), (2,[2]), (3,[99])],
  
  --Agregar sus propios tests.

  (insertWith (++) "1989" ["TBA 3", "TBA 4", "TBA 5"] vaultAlbums) ! "1989" ~=? ["TBA 1", "TBA 2", "TBA 3", "TBA 4", "TBA 5"],
  last (insertWith (++) "Reputation" ["SE VIENE"] vaultAlbums) ~=? ("Reputation", ["SE VIENE"])
  ]

songsAlbums = [("1989", "Welcome to New York"), ("1989", "Out of the woods"), ("1989", "Wildest dreams"), 
                ("Speak Now", "Mean"),  ("Speak Now", "Innocent"),  ("Speak Now", "Haunted")]

testsEj4 = test [
  (length $ groupByKey songsAlbums) ~=? 2,
  (sort $ (groupByKey songsAlbums) ! "1989") ~=? sort ["Welcome to New York","Out of the woods", "Wildest dreams"], 
  (sort $ (groupByKey songsAlbums) ! "Speak Now") ~=? sort ["Mean",  "Innocent",  "Haunted"]  
  ]

taylorVersionReleaseDate = [("Fearless", 2021), ("Speak Now", 2023), ("Red", 2021), ("1989", 2023)]


testsEj5 = test [
  (unionWith (+) [("rutas",3)] [("rutas", 4), ("ciclos", 1)]) ~=? [("rutas",7),("ciclos",1)],
  
   --Agregar sus propios tests.
  
  sort (unionWith (-) taylorVersionReleaseDate taylorAlbums) ~=? sort [("Taylor Swift", 2006), ("Fearless", 13), ("Speak Now", 13), ("Red", 9), ("1989", 9), 
                ("Reputation", 2017), ("Lover", 2019), ("Folklore", 2020), ("Evermore", 2020), ("Midnights", 2022)]
  ]

testsEj6 = test [
  (sort $ distributionProcess 5 [1 ,2 ,3 ,4 ,5 ,6 ,7 ,8 ,9 ,10 ,11 ,12]) ~=? sort [[1 ,6 ,11] ,[2 ,7 ,12] ,[3 ,8] ,[4 ,9] ,[5 ,10]], 

  (sort $ distributionProcess 3 taylorAlbums) ~=? sort [[("Taylor Swift", 2006), ("Red", 2012), ("Lover", 2019), ("Midnights", 2022)], 
  [("Fearless", 2008), ("1989", 2014), ("Folklore", 2020)],  [("Speak Now", 2010),("Reputation", 2017)  ,("Evermore", 2020)]], 

  (sort $ distributionProcess 4 $ vaultAlbums ! "Speak Now") ~=? sort [["Electric Touch", "Timeless"],  ["When Emma Falls in Love","I Can See You"],  ["Foolish One"], ["Castles Crumbling"]]
  
  ]

testsEj7 = test [
  mapperProcess mapperRestos [1, 5, 10, 25, 3, 14, 4] ~=? [(4,[4,14]),(3,[3]),(0,[25,10,5]),(1,[1])],
  
  --Agregar sus propios tests.
  
  mapperProcess (\(y,x) -> [(2000 + (mod (div x 10) 10)*10, y)]) taylorAlbums ~=? [(2020,["Midnights","Evermore","Folklore"]),(2010,["Lover","Reputation","1989","Red","Speak Now"]),(2000,["Fearless","Taylor Swift"])]
  ]

testsEj8 = test [
  (map (\(x,y)->(x,sort y)) $ combinerProcess palabras) ~=? [(1,["Chau","Hola","Saludos"]),(2,["Gato","Jirafa","Perro","Perro"]),(3,["Casa"]),(4,["Auto","Barco","Tren"])],
 
 --Agregar sus propios tests.

 (combinerProcess $ (groupByKey songsAlbums) : vaultAlbums : []) ~=? [("1989",["Wildest dreams","Out of the woods","Welcome to New York","TBA 1","TBA 2"]),
 ("Fearless",["You All Over Me","Mr. Perfectly Fine","That's When","We Were Happy","Don't You","Bye Bye Baby"]),
 ("Red",["Nothing New","Run","The Very First Night","Forever Winter","Babe","Better Man"]),
 ("Speak Now",["Haunted","Innocent","Mean","Electric Touch","When Emma Falls in Love","Foolish One","Castles Crumbling","Timeless","I Can See You"])]


  ]

testsEj9 = test [
  reducerProcess (\(x, xs)->x : nub xs)  [("Saludo:",["Chau","Hola","Saludos"]),("Mamífero:",["Gato","Jirafa","Perro","Perro"]),("Edificio:",["Casa"]),("Vehículo:",["Auto","Barco","Tren"])] ~=? ["Saludo:","Chau","Hola","Saludos","Mamífero:","Gato","Jirafa","Perro","Edificio:","Casa","Vehículo:","Auto","Barco","Tren"],
  
   --Agregar sus propios tests.

  (reducerProcess (\(x, xs)-> [xs]) songsAlbums) ~=? ["Welcome to New York","Out of the woods","Wildest dreams","Mean","Innocent","Haunted"]

  
  ]

testsEj10 = test [
  sort (visitasPorMonumento ["m1","m2","m3","m2"]) ~=? [("m1",1),("m2",2),("m3",1)],
  [("Argentina",2),("Irak",1),("SegundoFrancia",1)] ~=? sort (monumentosPorPais items),
  monumentosTop ["m3","m2","m2","m3","m1","m2","m3","m3","m4","m1"] ~=? ["m3","m2","m1","m4"],
  sort (restosMod5 [0,1,2,3,4,5,6,15,11]) ~=? [(0,3),(1,3),(2,1),(3,1),(4,1)],
  restosMod5 [] ~=? []
  
  

  
  ]
