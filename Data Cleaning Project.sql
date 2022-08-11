
--------Data Cleaning of the Nashville Housing Dataset on Sql server- Azure DAta Studio------

--------=====Skills used: Joins, Substrings, Charindex, Parse, Partition, CTE=====------

---Initial steps: Create table, set data types and import the csv dataset using the SQL Server Import extension---


--==View data==--

SELECT TOP (100) *
FROM dbo.nashvillehousing



--==Date format standardization (the csv file had some values infront of date but when importing i selected the date data type which rectified it)

SELECT saledate
FROM dbo.nashvillehousing

--Ways to standardize date format--

SELECT saleDate, CONVERT(Date,SaleDate)
FROM dbo.NashvilleHousing

UPDATE NashvilleHousing
SET SaleDate = CONVERT(Date,SaleDate)

--If it doesn't Update properly

ALTER TABLE NashvilleHousing
ADD SaleDateConverted Date;

UPDATE NashvilleHousing
SET SaleDateConverted = CONVERT(Date,SaleDate)




--==Address population for null propertyaddress==--

--display null address--
SELECT propertyaddress
FROM dbo.NashvilleHousing
WHERE propertyaddress IS NULL

SELECT *
FROM dbo.NashvilleHousing
WHERE propertyaddress IS NULL
ORDER BY parcelid

--now populate parcels ids with null address with the parcel ids with known address using JOIN, since there are similar parcelids with addresses-- 
SELECT a.parcelid, a.propertyaddress, b.parcelid, b.propertyaddress
FROM dbo.NashvilleHousing a
JOIN dbo.NashvilleHousing b
ON a.parcelid = b.parcelid
AND a.uniqueid <> b.uniqueid 
WHERE a.propertyaddress IS NULL

SELECT a.parcelid, a.propertyaddress, b.parcelid, b.propertyaddress, ISNULL(a.propertyaddress,b.propertyaddress)
FROM dbo.NashvilleHousing a
JOIN dbo.NashvilleHousing b
ON a.parcelid = b.parcelid
AND a.uniqueid <> b.uniqueid
WHERE a.propertyaddress IS NULL

UPDATE a
SET propertyaddress = ISNULL(a.propertyaddress,b.propertyaddress)
FROM dbo.NashvilleHousing a
JOIN dbo.NashvilleHousing b
ON a.parcelid = b.parcelid
AND a.uniqueid <> b.uniqueid
WHERE a.propertyaddress IS NULL

--check if update is effected--
SELECT a.parcelid, a.propertyaddress, b.parcelid, b.propertyaddress
FROM dbo.NashvilleHousing a
JOIN dbo.NashvilleHousing b
ON a.parcelid = b.parcelid
AND a.uniqueid <> b.uniqueid 
WHERE a.propertyaddress IS NOT NULL




--===Separating property and owner address into individual columns for address, city and state; using substrings and charindex===--
SELECT propertyaddress
FROM dbo.NashvilleHousing

--remove state--
SELECT
SUBSTRING(propertyaddress, 1, CHARINDEX(',', propertyaddress)) as Address
FROM dbo.nashvillehousing

--remove comma after address-
SELECT
SUBSTRING(propertyaddress, 1, CHARINDEX(',', propertyaddress)-1) as Address
FROM dbo.nashvillehousing

--remove comma before state-
SELECT
SUBSTRING(propertyaddress, 1, CHARINDEX(',', propertyaddress)-1) as Address
, SUBSTRING(propertyaddress, CHARINDEX(',', propertyaddress)+1, LEN(propertyaddress)) as Address
FROM dbo.nashvillehousing

--create 2 new columns for the newly split propertyaddress)
ALTER TABLE NashvilleHousing
ADD PropertySplitAddress NVARCHAR(200);

UPDATE NashvilleHousing
SET PropertySplitAddress = SUBSTRING(propertyaddress, 1, CHARINDEX(',', propertyaddress)-1)

ALTER TABLE NashvilleHousing
ADD PropertySplitCity  NVARCHAR(200);

UPDATE NashvilleHousing
SET PropertySplitCity = SUBSTRING(propertyaddress, CHARINDEX(',', propertyaddress)+1, LEN(propertyaddress))

--preview update--
SELECT *
FROM dbo.nashvillehousing



--==Owner Address Split using parsename==--

SELECT owneraddress
FROM dbo.nashvillehousing

--Replace periods with commas--
SELECT
PARSENAME(REPLACE(OwnerAddress, ',', '.'), 3) 
, PARSENAME(REPLACE(OwnerAddress, ',', '.'), 2) 
, PARSENAME(REPLACE(OwnerAddress, ',', '.'), 1) 
FROM dbo.nashvillehousing

--create 3 new columns for the newly split owneraddress)
ALTER TABLE NashvilleHousing
ADD OwnerSplitAddress NVARCHAR(200);

UPDATE NashvilleHousing
SET OwnerSplitAddress = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 3)

ALTER TABLE NashvilleHousing
ADD OwnerSplitCity  NVARCHAR(200);

UPDATE NashvilleHousing
SET OwnerSplitCity = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 2)

ALTER TABLE NashvilleHousing
ADD OwnerSplitState  NVARCHAR(200);

UPDATE NashvilleHousing
SET OwnerSplitState = PARSENAME(REPLACE(OwnerAddress, ',', '.'), 1)

--preview update--
SELECT *
FROM dbo.nashvillehousing



--===Update entries Y and N to Yes and No in column soldasvacant===--
--count number of each entry--
SELECT DISTINCT (soldasvacant), COUNT(soldasvacant)
FROM dbo.NashvilleHousing
GROUP BY soldasvacant
ORDER BY 2
--ORDER BY COUNT(soldasvacant) DESC

--replace Y with Yes and N with No--
SELECT soldasvacant
, CASE WHEN soldasvacant = 'Y' THEN 'Yes'
  WHEN soldasvacant = 'N' THEN 'No'
  ELSE soldasvacant
  END
FROM dbo.NashvilleHousing

--update the change--
UPDATE nashvillehousing
SET soldasvacant = CASE WHEN soldasvacant = 'Y' THEN 'Yes'
  WHEN soldasvacant = 'N' THEN 'No'
  ELSE soldasvacant
  END

--preview update--
SELECT DISTINCT (soldasvacant), COUNT(soldasvacant)
FROM dbo.NashvilleHousing
GROUP BY soldasvacant
ORDER BY 2




--==Remove Duplicate entries using partition, order rank, row numbers, CTE==--

--spool out the duplicate rows, delete with code below and rerun this to confirm--
WITH RowNumCTE AS(
SELECT *,
    ROW_NUMBER() OVER (
    PARTITION BY parcelid,
                 propertyaddress,
                 saleprice,
                 saledate,
                 legalreference
                 ORDER BY
                    uniqueid
                    ) row_num
FROM dbo.nashvillehousing
--ORDER BY parcelid
)
SELECT *
FROM RowNumCTE
WHERE row_num > 1
ORDER BY PropertyAddress

--Delete the duplicate rows--
WITH RowNumCTE AS(
SELECT *,
    ROW_NUMBER() OVER (
    PARTITION BY parcelid,
                 propertyaddress,
                 saleprice,
                 saledate,
                 legalreference
                 ORDER BY
                    uniqueid
                    ) row_num
FROM dbo.nashvillehousing
--ORDER BY parcelid
)
DELETE
FROM RowNumCTE
WHERE row_num > 1




--==**Delete unused columns**==--
SELECT *
FROM dbo.nashvillehousing

ALTER TABLE dbo.nashvillehousing
DROP COLUMN owneraddress, propertyaddress,taxdistrict, saledate

ALTER TABLE dbo.nashvillehousing
DROP COLUMN saledate












