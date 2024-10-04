-- observing the data
select *
from layoffs

-- creating duplicate table
create table layoffs_staging
like layoffs;

insert layoffs_staging
select * 
from layoffs ;

select * from layoffs_staging;

-- checking for duplicate rows

with duplicate_cte as (select *,
ROW_NUMBER() OVER (partition by company,location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging)
select * 
from duplicate_cte
where row_num >1;


alter  table layoffs_staging
add column row_num integer;


truncate table layoffs_staging ;

insert into layoffs_staging
select *, ROW_NUMBER() OVER (partition by company,location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions)
FROM layoffs;

select * from layoffs_staging ls ;

-- deleting duplicates
delete from layoffs_staging
where row_num > 1;


-- Standardizing data

select company, trim(company)
from layoffs_staging;

update layoffs_staging
set company = trim(company);

select distinct industry
from layoffs_staging ls 
order by 1;

-- standardising spelling of Crypto
update layoffs_staging 
set industry = 'Crypto'
where industry like 'Crypto%';

select distinct country 
from layoffs_staging ls 
order by 1;

-- correcting spelling of United States
update layoffs_staging 
set country = trim(trailing '.' from country)
where country like 'United States%';

select date, str_to_date(date, '%m/%d/%Y') 
from layoffs_staging;

-- converting string value to date
update layoffs_staging
set `date` = str_to_date(`date`, '%m/%d/%Y')
where `date` != 'NULL';

-- converting null string to null
update layoffs_staging
set `date` = null
where `date` = 'NULL'


alter table layoffs_staging
modify column date DATE;



-- searching for null values
select * from layoffs_staging
where industry = 'NULL'
or industry = ''


select * from layoffs_staging
where company  = "Bally's Interactive"

-- searching for industry values from given data
select * from layoffs_staging t1
join layoffs_staging t2
on t1.company = t2.company
where (t1.industry = 'NULL'
or t1.industry = '')
and t2.industry is not null;

-- converting string null to null
update layoffs_staging  
set industry = null 
where industry = 'NULL'
or industry = ''

-- populating industry values
update layoffs_staging t1
join layoffs_staging t2
on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null 
and t2.industry is not null;


select * from layoffs_staging ls n
where total_laid_off = 'NULL'
and percentage_laid_off = 'NULL'

-- deleting null values
delete from layoffs_staging 
where total_laid_off = 'NULL'
and percentage_laid_off = 'NULL'

select * from layoffs_staging ls 

-- dropping unrequired column
alter table layoffs_staging 
drop column row_num;


-- setting string null values to null
update layoffs_staging 
set funds_raised_millions = null 
where funds_raised_millions = 'NULL'

update layoffs_staging 
set total_laid_off = null 
where total_laid_off = 'NULL'

update layoffs_staging 
set percentage_laid_off = null 
where percentage_laid_off = 'NULL'

update layoffs_staging 
set stage = null 
where stage = 'NULL'




