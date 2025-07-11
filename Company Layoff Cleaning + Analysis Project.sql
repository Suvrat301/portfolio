SELECT *  FROM layoffs;

-- DATA CLEANING

-- STEPS
-- 1. REMOVE DUPLICATE ENTRIES
-- 2. STANDARDIZE DATA
-- 3. REMOVE NULL AND BLANK ENTRIES
-- 4. REMOVE UNWANTED COLUMNS

-- 1. REMOVING DUPLICATES

CREATE TABLE layoff_exp
LIKE layoffs;

SELECT * FROM layoff_exp;

INSERT INTO layoff_exp
SELECT * FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, date) AS row_num
FROM layoff_exp;

WITH first_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoff_exp
)
SELECT * 
FROM first_cte
WHERE row_num > 1;



CREATE TABLE `layoff_exp2` (
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


INSERT INTO layoff_exp2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoff_exp;
 
 DELETE FROM layoff_exp2
 WHERE row_num > 1;
 
 -- 2. STANDARDIZING DATA
 
 SELECT company, TRIM(company)
 FROM layoff_exp2;
 
 UPDATE layoff_exp2
 SET company = Trim(company);
 
 SELECT DISTINCT (industry)
 FROM layoff_exp2;
 
 UPDATE layoff_exp2
 SET INDUSTRY = 'Crypto'
 WHERE industry LIKE 'Cryptp';

 SELECT DISTINCT (country)
 FROM layoff_exp2
 ORDER BY 1;
 
 UPDATE layoff_exp2
 SET country = TRIM(TRAILING '.' FROM country)
 WHERE country LIKE 'United States%';
 
 SELECT `date`,
 STR_TO_DATE(`date`,' %m/%d/%Y')
 FROM layoff_exp2;
 
 UPDATE layoff_exp2
 SET date = STR_TO_DATE(`date`,' %m/%d/%Y');
 
 ALTER TABLE layoff_exp2
 MODIFY COLUMN `date` DATE;
 
 SELECT `date`
 FROM layoff_exp2;
 
 -- 3. REMOVING NULLS AND BLANK ENTRIES
 
 SELECT * FROM layoff_exp2;
 
 SELECT * FROM layoff_exp2
 WHERE total_laid_off IS NULL
 AND percentage_laid_off IS NULL;
 
 SELECT * FROM layoff_exp2
 WHERE industry IS NULL 
 OR industry = '';
 
 UPDATE layoff_exp2
 SET industry = NULL 
 WHERE industry = '';
 
 SELECT * FROM layoff_exp2 t1
 JOIN  layoff_exp2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

UPDATE layoff_exp2 t1
JOIN  layoff_exp2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL)
AND t2.industry IS NOT NULL;

-- 4. removing blank or useless columns or rows

DELETE FROM layoff_exp2
 WHERE total_laid_off IS NULL
 AND percentage_laid_off IS NULL;
 
 ALTER TABLE layoff_exp2
 DROP column row_num;
 
 SELECT * FROM layoff_exp2;
 

-- EXPLORING DATA 

SELECT * FROM layoff_exp2;

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoff_exp2;

SELECT * FROM layoff_exp2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT country, SUM(total_laid_off)
FROM layoff_exp2
GROUP BY country
ORDER BY 2 DESC;

SELECT * FROM layoff_exp2;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoff_exp2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;

SELECT stage, SUM(total_laid_off)
FROM layoff_exp2
GROUP BY stage
ORDER BY 1 DESC;

SELECT company, SUM(percentage_laid_off)
FROM layoff_exp2
GROUP BY company
ORDER BY 2 DESC;

SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoff_exp2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY`MONTH`
ORDER BY 1 ASC;

WITH rolling_total AS (
SELECT SUBSTRING(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoff_exp2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY`MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_off, SUM(total_off) OVER (ORDER BY `MONTH`) AS rolling_total
FROM rolling_total;

SELECT company, SUM(total_laid_off)
FROM layoff_exp2
GROUP BY company
ORDER BY 2 DESC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoff_exp2
GROUP BY company, YEAR(`date`)
ORDER BY company ASC;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoff_exp2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;

WITH company_year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoff_exp2
GROUP BY company, YEAR(`date`)
), company_year_ranking AS (
SELECT * , DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
FROM company_year
WHERE years IS NOT NULL
)
SELECT * FROM company_year_ranking
WHERE ranking <= 5;