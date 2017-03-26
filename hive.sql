DROP TABLE stock;
create table stock(date string , open float , high float , low float , close float, volume string , adjclose float ) row format delimited fields terminated by ',' lines terminated by '\n'
location 'hdfs:///data'
tblproperties ("skip.header.line.count"="1");
DROP TABLE StockName;
create table StockName(filename string ,date string, keydate string, adjclose float );
INSERT INTO TABLE StockName SELECT regexp_replace(INPUT__FILE__NAME,'.*\//.*\/',''),date,substr(Date,0,7), adjclose FROM stock;
DROP TABLE stock;
DROP TABLE xi;
create table xi(file string , keydate string, first_day string, last_day  string);
INSERT INTO TABLE xi SELECT filename,keydate,min(date),max(date) FROM StockName group by filename,keydate;
DROP TABLE rr;
create table rr(file string , adjclose_min float, adj_close_max float, rr float);
INSERT INTO TABLE rr SELECT t.file,s1.adjclose,s2.adjclose,(s2.adjclose-s1.adjclose)/s1.adjclose from xi as t
join StockName as s1 on(s1.filename=t.file and s1.keydate=t.keydate)
join StockName as s2 on(s2.filename=t.file and s2.keydate=t.keydate)
where s1.date=t.first_day and s2.date = t.last_day;
DROP TABLE StockName;
DROP TABLE xi;
DROP TABLE vol;
create table vol(file string, volatility float);
INSERT INTO TABLE vol select file, stddev_samp(rr) as volatility from rr group by file;
DROP TABLE rr;
DROP TABLE stockvol;
create table stockvol as select file,volatility, rank() over (order by volatility) as Min_stocks, rank() over (order by volatility DESC) as Max_stocks from vol where volatility > 0.0;







