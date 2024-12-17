#-------------EXPLORATORY DATA ANALYSIS (EDA)-------------
SELECT *
FROM layoffs_staging2;


SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;


SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY total_laid_off DESC;

SELECT company, SUM(total_laid_off) AS Total_laid_off
FROM layoffs_staging2
GROUP BY company
ORDER BY Total_laid_off DESC;


SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;


SELECT industry, SUM(total_laid_off) AS Total_laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY Total_laid_off DESC;


SELECT country, SUM(total_laid_off) AS Total_laid_off
FROM layoffs_staging2
GROUP BY country
ORDER BY Total_laid_off DESC;


SELECT `date`, SUM(total_laid_off) AS Total_laid_off
FROM layoffs_staging2
GROUP BY `date`
ORDER BY `date` DESC;


SELECT YEAR(`date`), SUM(total_laid_off) AS Total_laid_off
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY YEAR(`date`) DESC;


SELECT stage, SUM(total_laid_off) AS Total_laid_off
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;


SELECT SUBSTRING(`date`,1,7) AS `PERIOD`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `PERIOD`
ORDER BY 1 
;


WITH Rolling_Total AS
(
	SELECT SUBSTRING(`date`,1,7) AS `PERIOD`, SUM(total_laid_off) AS Total
	FROM layoffs_staging2
	WHERE SUBSTRING(`date`,1,7) IS NOT NULL
	GROUP BY `PERIOD`
	ORDER BY 1 
)
SELECT `PERIOD`, Total, SUM(Total) OVER(ORDER BY `PERIOD`) AS rolling_total_laid_off
FROM Rolling_Total
;


SELECT company, YEAR(`date`), SUM(total_laid_off) AS Total_laid_off
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY Total_laid_off DESC;


WITH company_year (company, years, Total_laid_off) AS
(
	SELECT company, YEAR(`date`), SUM(total_laid_off) AS Total_laid_off
	FROM layoffs_staging2
	GROUP BY company, YEAR(`date`)
)
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY Total_laid_off DESC) AS ranking
FROM company_year
WHERE years IS NOT NULL
ORDER BY ranking ASC;


WITH company_year (company, years, Total_laid_off) AS
(
	SELECT company, YEAR(`date`), SUM(total_laid_off) AS Total_laid_off
	FROM layoffs_staging2
	GROUP BY company, YEAR(`date`)
),
company_year_rank AS
(
SELECT *, DENSE_RANK() OVER(PARTITION BY years ORDER BY Total_laid_off DESC) AS ranking
FROM company_year
WHERE years IS NOT NULL
)
SELECT *
FROM company_year_rank
WHERE ranking <= 5
;

SELECT industry, SUM(total_laid_off) as TotalLayOff
FROM layoffs_staging2
GROUP BY industry
ORDER BY TotalLayOff DESC
LIMIT 10;

WITH layoff_by_industry AS
(
SELECT industry, YEAR(`date`) AS yearToCheck, SUM(total_laid_off) AS TotalLayOff
FROM layoffs_staging2
WHERE YEAR(`date`) IS NOT NULL
GROUP BY industry, YEAR(`date`)
),
ranking_cte AS
(
SELECT *, dense_rank() OVER(PARTITION BY yearToCheck ORDER BY TotalLayOff DESC) AS ranking
FROM layoff_by_industry
ORDER BY ranking
)
SELECT *
FROM ranking_cte 
WHERE ranking <= 3;

