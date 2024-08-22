use world_layoffs;
select * from layoffs; 

-- 1. remove duplicate from table.
-- 2. standardize the data.
-- 3. null values or blank values.
-- 4 remove  and columns

 create table layoffs_staging  -- coping  data from layoffs to layoffs_staginf
 like layoffs;

select * from layoffs_staging; 

insert layoffs_staging
select * from layoffs;


 select *,
 row_number() over(
 partition by company,industry,total_laid_off,percentage_laid_off,'date') as row_num
 from layoffs_staging; 

use world_layoffs;
with duplicate_cte as
(
select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions )as row_num

from layoffs_staging
)
select *from duplicate_cte
where row_num > 1;
 
 
select *

from layoffs_staging
where company="casper";

with duplicate_cte as
(
select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions )as row_num

from layoffs_staging
)
select *from duplicate_cte
where row_num > 1;
 
 
delete

from layoffs_staging
where company="casper";


 
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
 
 select * from layoffs_staging2
 where row_num >1;

 insert into layoffs_staging2
 select *,
row_number() over(
partition by company,location,industry,total_laid_off,percentage_laid_off,'date',stage,country,funds_raised_millions )as row_num
from layoffs_staging;

delete 
from layoffs_staging2
where row_num > 1;

select * from layoffs_staging2;

-- standardizing data 
 select company,trim(company)
 from layoffs_staging2; 
 
 update layoffs_staging2 
 set company = trim(company);
 
 select  distinct industry 
 from layoffs_staging2;
 
 select * from layoffs_staging2
 where industry like 'crypto%';

update layoffs_staging2 
set industry = 'crypto'
where industry like 'crypto%';

select distinct location
from layoffs_staging2
order by 1;
 

select * from layoffs_staging2
where country like 'united states'
order by 1;

select distinct country,trim(trailing '.' from  country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'united state';

-- select 'date' ,
-- str_to_date(date,'%y/%m/%d')
-- from layoffs_staging2;





 select *from layoffs_staging2;
 
 select * from layoffs_staging2
 where total_laid_off is null and percentage_laid_off is null;
 
 update layoffs_staging2
 set industry= null
 where industry=' ';
 select distinct industry
 from layoffs_staging2;
 
 select *from layoffs_staging2
  where industry is null
  or industry=' ';
 
 select t1.industry,t2.industry
 from layoffs_staging2 t1
 join layoffs_staging2 t2
 on t1.company =t2.company   

where (t1.industry is null or t1.industry='')
and t2.industry is not null;

select*from layoffs_staging
where company like 'bally%';




UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

select * from layoffs_staging;



select *from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


delete from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_staging2
drop column row_num;

select * from layoffs_staging;

 