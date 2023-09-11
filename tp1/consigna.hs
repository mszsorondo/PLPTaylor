module MapReduce where

import Data.Ord
import Data.List
import Test.HUnit

-- ---------------------------------Sección 1---------Diccionario ---------------------------
type Dict k v = [(k,v)]

-- Ejercicio 1
belongs :: Eq k => k -> Dict k v -> Bool
belongs = undefined

(?) :: Eq k => Dict k v -> k -> Bool
(?) = undefined
--Main> [("calle",[3]),("city",[2,1])] ? "city" 
--True

-- Ejercicio 2
get :: Eq k => k -> Dict k v -> v
get = undefined

(!) :: Eq k => Dict k v -> k -> v
(!) = undefined
--Main> [("calle",[3]),("city",[2,1])] ! "city" 
--[2,1]

-- Ejercicio 3
insertWith :: Eq k => (v -> v -> v) -> k -> v -> Dict k v -> Dict k v
insertWith = undefined
--Main> insertWith (++) 2 ['p'] (insertWith (++) 1 ['a','b'] (insertWith (++) 1 ['l'] []))
--[(1,"lab"),(2,"p")]

-- Ejercicio 4
groupByKey :: Eq k => [(k,v)] -> Dict k [v]
groupByKey = undefined

-- Ejercicio 5
unionWith :: Eq k => (v -> v -> v) -> Dict k v -> Dict k v -> Dict k v
unionWith = undefined
--Main> unionWith (++) [("calle",[3]),("city",[2,1])] [("calle", [4]), ("altura", [1,3,2])]
--[("calle",[3,4]),("city",[2,1]),("altura",[1,3,2])]


-- ------------------------------Sección 2--------------MapReduce---------------------------

type Mapper a k v = a -> [(k,v)]
type Reducer k v b = (k, [v]) -> [b]

-- Ejercicio 6
distributionProcess :: Int -> [a] -> [[a]]
distributionProcess = undefined

-- Ejercicio 7
mapperProcess :: Eq k => Mapper a k v -> [a] -> Dict k [v]
mapperProcess = undefined

-- Ejercicio 8
combinerProcess :: (Eq k, Ord k) => [Dict k [v]] -> Dict k [v]
combinerProcess = undefined

-- Ejercicio 9
reducerProcess :: Reducer k v b -> Dict k [v] -> [b]
reducerProcess = undefined

-- Ejercicio 10
mapReduce :: (Eq k, Ord k) => Mapper a k v -> Reducer k v b -> [a] -> [b]
mapReduce = undefined


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
      ("country", "Francia"),
      ("latlong", "-24.6033,-18.3817")]),
    (Monument, [
      ("name", "Bagdad Bridge"),
      ("country", "Irak"),
      ("new_field", "new"),
      ("latlong", "-11.6033,-12.3817")])
    ]


------------------------------------------------
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
  ([("calle",[3]),("ciudad",[2,1])] ? "perro")  ~=? False --Agregar sus propios tests.
  ]

testsEj2 = test [
  [("calle","San Blas"),("ciudad","Hurlingham")] ! "ciudad" ~=? "Hurlingham" --Agregar sus propios tests.
  ]

testsEj3 = test [
  (insertWith (++) 1 [99] [(1, [1]), (2, [2])]) ~=? [(1,[1,99]),(2,[2])] --Agregar sus propios tests.
  ]

testsEj4 = test [
  0 ~=? 0 --Cambiar esto por tests verdaderos.
  ]

testsEj5 = test [
  (unionWith (+) [("rutas",3)] [("rutas", 4), ("ciclos", 1)]) ~=? [("rutas",7),("ciclos",1)] --Agregar sus propios tests.
  ]

testsEj6 = test [
  0 ~=? 0 --Cambiar esto por tests verdaderos.
  ]

testsEj7 = test [
  mapperProcess mapperRestos [1, 5, 10, 25, 3, 14, 4] ~=? [(4,[4,14]),(3,[3]),(0,[25,10,5]),(1,[1])] --Agregar sus propios tests.
  ]

testsEj8 = test [
  (map (\(x,y)->(x,sort y)) $ combinerProcess palabras) ~=? [(1,["Chau","Hola","Saludos"]),(2,["Gato","Jirafa","Perro","Perro"]),(3,["Casa"]),(4,["Auto","Barco","Tren"])]
 --Agregar sus propios tests.
  ]

testsEj9 = test [
  reducerProcess (\(x, xs)->x : nub xs)  [("Saludo:",["Chau","Hola","Saludos"]),("Mamífero:",["Gato","Jirafa","Perro","Perro"]),("Edificio:",["Casa"]),("Vehículo:",["Auto","Barco","Tren"])] ~=? ["Saludo:","Chau","Hola","Saludos","Mamífero:","Gato","Jirafa","Perro","Edificio:","Casa","Vehículo:","Auto","Barco","Tren"] --Agregar sus propios tests.
  ]

testsEj10 = test [
  sort (visitasPorMonumento ["m1","m2","m3","m2"]) ~=? [("m1",1),("m2",2),("m3",1)],
  [("Argentina",2),("Irak",1)] ~=? sort (monumentosPorPais items),
  monumentosTop ["m3","m2","m2","m3","m1","m2","m3","m3","m4","m1"] ~=? ["m3","m2","m1","m4"] --Agregar sus propios tests.
  ]
