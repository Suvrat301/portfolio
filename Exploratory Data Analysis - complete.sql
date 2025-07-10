-- EXPLORATORY DATA ANALYSIS

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