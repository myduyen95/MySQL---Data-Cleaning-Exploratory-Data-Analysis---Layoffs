#-------------DATA CLEANING PROJECT--------------------
SELECT *
FROM layoffs;

#--1. Remove layoffs duplicates
#--2. Standardize the data
#--3. Null values or blank values
#--4. Remove unnecessary columns in layoffs

# We need to make alot of amendments in data. If we do directly with raw data (table layoffs) and in case we make any mistakes, it will create troubles.
# Hence, create a staging table is a good way. In any cases, we can refer back to raw data table.

# Create a staging table
CREATE TABLE layoffs_staging
LIKE layoffs;

# Staging table is created that has columns similar as raw data table - layoffs
SELECT *
FROM layoffs_staging;

# Insert data to the staging table
INSERT layoffs_staging
SELECT *
FROM layoffs;


#--1. Remove duplicates
SELECT *,
	ROW_NUMBER() OVER(
		PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
        ) AS row_num
FROM layoffs_staging;
	
WITH duplicate_cte AS
(
	SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
            ) AS row_num
	FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

# To test if SQL query above checking duplicated item is correct
SELECT *
FROM layoffs_staging
WHERE company = 'casper';


# As we cant delete duplicated rows from staging table, we need to create staging table 2 that includes column 'row_num' that is used to delete duplicated rows
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;


INSERT INTO layoffs_staging2
SELECT *,
		ROW_NUMBER() OVER(
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
            ) AS row_num
FROM layoffs_staging;	

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

#After duplicated rows are deleted, we cannot see them any more
SELECT *
FROM layoffs_staging2
WHERE row_num > 1;


# Staging table 2 after removing duplicated rows
SELECT *
FROM layoffs_staging2;



#--2. Standardize the data: finding issues in your data and fixing it

SELECT *
FROM layoffs_staging2;

#Trim company
UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT company
FROM layoffs_staging2;


#Check if industry has any issue
SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY industry;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';


#Check if location has any issue
SELECT DISTINCT location
FROM layoffs_staging2
ORDER BY location;


#Check if country has any issue
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY country;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States%';


SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;


UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';


#Convert 'date' from text format to date format
SELECT `date`,	
STR_TO_DATE(`date`,'%m/%d/%Y')			#Convert string to date format. The text is initially in the form of m/d/Y. If use %M instead of %m, it wont work, same for d and Y
FROM layoffs_staging2;


UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`,'%m/%d/%Y');
#After update, definition of date is still text but it is in date format

#Now we will convert its definition to date
ALTER TABLE layoffs_staging2           #This will completely change format of the table, so NEVER do this on raw table
MODIFY COLUMN `date` DATE;


#--3. Null values or blank layoffs values
# Check industry 
SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
	OR industry = '';

#Select 1 item for example
SELECT *
FROM layoffs_staging2
WHERE company = 'Airbnb';

#Set blank value to Null value first
UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

#We can get industry under same company and same location and post it to the same company and location where missing industry
SELECT t1.company, t1.location, t1.industry, t2.company, t2.location, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company 
		AND t1.location = t2.location
WHERE t1.industry IS NULL
	AND t2.industry IS NOT NULL;


UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company 
		AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
	AND t2.industry IS NOT NULL;

#For record that has no similar company and location, leave it as it is
SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Bally%';


#--4. Remove unnecessary columns in layoffs
# If total_laid_off and percentage_laid_off are blank, it does not make sense to keep these records in the table
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
	AND percentage_laid_off IS NULL;


DELETE 
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
	AND percentage_laid_off IS NULL;

#Column row_num is also no longer required
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;


#Final result - cleaned data
SELECT *
FROM layoffs_staging2;



