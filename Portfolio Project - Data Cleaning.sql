-- SQL Project - Data Cleaning


SELECT *
FROM layoffs;



-- creating a staging table where we will be working to clean the data

CREATE TABLE layoffs_staging 
LIKE layoffs;


INSERT layoffs_staging
SELECT *
FROM layoffs;



-- now we will be following the steps given below to clean the data
-- 1. checking for duplicates and removing(if any)
-- 2. standardizing the given data and fixing errors
-- 3. Looking at null values and fixing them
-- 4. removing unnecessary rows and columns





-- 1. Removing duplicates -----------------------------------------------------------------------------------------------------------------


-- checking for duplicates

SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised_millions) AS row_num                                               
FROM layoffs_staging;                                                                  


-- creating a CTE from the above query where row_num>1 to find the duplicate entries

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised_millions) AS row_num                                               
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num>1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,
country,funds_raised_millions) AS row_num                                               
FROM layoffs_staging;


-- now deleting entries where row_num>1 as these are duplicate values

DELETE
FROM layoffs_staging2
WHERE row_num>1;





-- 2. Standardizing Data ------------------------------------------------------------------------------------------------------------------


SELECT company,TRIM(company)
FROM layoffs_staging2;

-- updating the table by removing the white spaces in the "company" column

UPDATE layoffs_staging2
SET company=TRIM(company);         

-- Crypto has multiple different variations in the "industry" column                                      

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- So we will be updating the table by setting all of them to "Crypto" 

UPDATE layoffs_staging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto%';


-- we have both "United States" and "United States." in the "country" column.So we will be fixing it.

SELECT DISTINCT(country)
FROM layoffs_staging2
ORDER BY country;

-- removing the period after "United States" in the "country" column and updating the table

SELECT DISTINCT(country), TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country=TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- fixing the "date" column in YMD(Year,Month,Date) format

SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y')                                 
FROM layoffs_staging2;

-- updating the "date" column

UPDATE layoffs_staging2
SET `date`=STR_TO_DATE(`date`,'%m/%d/%Y');

-- converting the data type properly

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- checking if there are any null values or empty rows in "industry" column                            

SELECT DISTINCT(industry)
FROM layoffs_staging2
ORDER BY industry;  

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- updating all the blank values to Null in the "industry" column

UPDATE layoffs_staging2
SET industry=NULL
WHERE industry='';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL 
OR industry = ''
ORDER BY industry;

-- having a look at these

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';

SELECT *
FROM layoffs_staging2
WHERE company='Airbnb';

-- it looks like that airbnb belongs to "travel" industry, but here one entry isn't populated. It may be the same for the others
-- now we will be writing a query that if there is another row with the same company name, it will update it to the non-null industry values


SELECT t1.industry,t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
 ON t1.company=t2.company
WHERE (t1.industry IS NULL OR t1.industry='')
AND t2.industry IS NOT NULL;

-- updating the nulls in t1 by copying available industry value from t2 

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
 ON t1.company=t2.company
SET t1.industry=t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


-- It seems like "Bally's Interactive" is the only company left where nulls are not populated

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';     

 
       


-- 3. Looking at Null Values --------------------------------------------------------------------------------------------------------------


-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all are looking normal. I don't think I should change them
-- because having them null makes it easier for calculations while performing Exploratory Data Analysis

-- so there isn't anything I want to change with the null values





-- 4. Removing rows/columns that are not relevant -----------------------------------------------------------------------------------------


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- deleting rows where total_laid_off and perecntage_laid_off entries are null, as they are not relevant

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2;

-- droping the row_num column from our table

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;





















