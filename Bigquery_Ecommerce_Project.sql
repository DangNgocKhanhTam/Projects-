-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0

--Lưu ý chung: với Bigquery thì mình có thể groupby, orderby 1,2,3(1,2,3() ở đây là thứ tự của column mà mình select nhé

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
SELECT
        distinct format_date("%Y%m",parse_date("%Y%m%d",date)) as month, --phần này e k cần distinct, vì ở dưới mình có group by rồi, e group by month lại thành 3 dòng
        sum(totals.visits) as visits,                                    --tương tự mấy câu dưới nha
        sum(totals.pageviews) as pageviews,
        sum(totals.transactions) as transactions,
        sum(totals.totalTransactionRevenue)/ power(10,6) as revenue
FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where   _table_suffix between '0101' and '0331'
group by month --group by 1
order by month  --order by 1    ---tương tự mấy phần dưới nha



-- Query 02: Bounce rate per traffic source in July 2017
SELECT
        trafficSource.source,
        sum(totals.visits) as total_visits, 
        sum(totals.bounces) as total_no_of_bounces,
        (sum(totals.bounces)/sum(totals.visits))*100 as bounce_rate
      
FROM
        `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
group by trafficSource.source
order by total_visits desc



-- Query 3: Revenue by traffic source by week, by month in June 2017
--gặp những bài yêu cầu lấy week và month, highest và shortest thì a sẽ tách làm 2 cte
--mình thứ làm thử 1 cte trước, rồi copy paste làm tiếp những cte khác, rồi mình join hoặc union lại vs nhau = 1 key nào đó

with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)

select * from month_data
union all
select * from week_data

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
with    pageviews_purchase as (
select  
        distinct format_date( '%Y%m',parse_date("%Y%m%d", date)) as month1,
        (sum(totals.pageviews)/count(distinct fullVisitorId)) as avg_pageviews_purchase
        
from   `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where   _table_suffix between '0601' and '0731'
and     totals.transactions >= 1
group by month1) ,   
                                    --ở 3 cte, e ghi là month cũng đc, k gần month1 month2
pageviews_non_purchase as (
select  
        distinct format_date( '%Y%m',parse_date("%Y%m%d", date)) as month2,
        (sum(totals.pageviews)/count(distinct fullVisitorId)) as  avg_pageviews_non_purchas
        
from   `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
where   _table_suffix between '0601' and '0731'
and     totals.transactions is Null
group by month2  )

SELECT
        distinct format_date( '%Y%m',parse_date("%Y%m%d", date)) as month3,
        pageviews_purchase.avg_pageviews_purchase,
        pageviews_non_purchase.avg_pageviews_non_purchase
FROM
        (`bigquery-public-data.google_analytics_sample.ga_sessions_2017*`) as c
inner join pageviews_non_purchase np
on         c.month3 = pageviews_non_purchase.month2   --khúc này e ghi c.month3, nó k chạy đc, thì month3 lúc này đang là alias thoi, k phải là 1 field
inner join pageviews_purchase p                      --nếu đúng thì phải ghi là c.format_date( '%Y%m',parse_date("%Y%m%d", date)) = month2
on         c.month3 = pageviews_purchase.month1  
where      _table_suffix between '0601' and '0731'
group by    month3;
        
(em dungf cte de viet ma bi loi, em chay tung cai nho thi chay dc toi khi ghep vao thi chay khong dc) 
--> hình như ở giữa e cte của e k có dấu phẩy, nên k chạy đc á
--đây là câu query của a
with purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

select  
        distinct format_date( '%Y%m',parse_date("%Y%m%d", date)) as month ,
        (sum(totals.transactions)/count(distinct fullVisitorId)) as Avg_total_transactions_per_user
from   `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where   totals.transactions >=1
group by month 
        
-- Query 06: Average amount of money spent per session
#standardSQL

select  
        distinct format_date( '%Y%m',parse_date("%Y%m%d", date)) as month ,
        (sum(totals.totalTransactionRevenue)/count( fullVisitorId)) as avg_revenue_by_user_per_visit
from   `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
where   totals.transactions is not null
group by month 

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

SELECT 
        product.v2ProductName as other_purchased_products,
        sum(product.productQuantity) as quantity

FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
unnest (hits)as hits,
unnest (product) as product 
where   product.v2ProductName <> "YouTube Men's Vintage Henley"
and     product.productRevenue is not null
group by other_purchased_products

(cau nay ra output khong giong expected output) 
ah, bài này e chưa hiểu ý đề bài á, bài đang muốn mình tìm ra những sản phẩm (other product) mà được mua bởi những người từng mua sp Youtube
thì step1 là e sẽ lấy ra list những ng mua sản phẩm Youtube trước, gọi list id này là (1) nha, thì trong list (1) này có những ng chỉ mua youtube
nhưng cũng có người vừa mua youtube, vừa mua những sản phẩm khác(other product), thì để bài đang muốn mình tìm ra other product, và số lượg của nó

with buyer_list as(
    SELECT
        distinct fullVisitorId
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
    , UNNEST(hits) AS hits
    , UNNEST(hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley"
    AND totals.transactions>=1
    AND product.productRevenue is not null
)

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
JOIN buyer_list using(fullVisitorId)
WHERE product.v2ProductName != "YouTube Men's Vintage Henley"
 and product.productRevenue is not null
GROUP BY other_purchased_products
ORDER BY quantity DESC

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
bỏ qua phần calculate cohort map
ý bài này là 1 sản phẩm sẽ qua 3 giai đoạn, view -> add_to_cart -> purchare ; thì bài đang muốn mình đang tính đc ở mỗi giai đoạn, số lượng nó rớt còn bao nhiêu %
thì ý tưởng là e sẽ tạo ra 3 cte tính số liệu của 3 giai đoạn này, rồi sao đó ghép vs nhau thành 1 data thống nhất. thì điểm chung là thời gian, nên mình sẽ dùng time để mapping các cte lại

with
product_view as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '2'
GROUP BY 1
),

add_to_cart as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '3'
GROUP BY 1
),

purchase as(
SELECT
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  count(product.productSKU) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
, UNNEST(hits) AS hits
, UNNEST(hits.product) as product
WHERE _TABLE_SUFFIX BETWEEN '20170101' AND '20170331'
AND hits.eCommerceAction.action_type = '6'
group by 1
)

select
    pv.*,
    num_addtocart,
    num_purchase,
    round(num_addtocart*100/num_product_view,2) as add_to_cart_rate,
    round(num_purchase*100/num_product_view,2) as purchase_rate
from product_view pv
join add_to_cart a on pv.month = a.month
join purchase p on pv.month = p.month
order by pv.month


Cách 2: bài này mình có thể dùng count(case when) hoặc sum(case when)

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data

