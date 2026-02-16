import pandas as pd
import numpy as np
import mysql.connector
import seaborn as sns
import matplotlib.pyplot as plt

db = mysql.connector.connect(host='127.0.0.1',user='root',password='525745',database='ecommerce')
cur = db.cursor()
#List all unique cities where customers are located.
query = """SELECT DISTINCT customer_city FROM customers"""
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data, columns=['customer_city'])
print("Unique customer cities:")
for i, city in enumerate(df['customer_city'], start=1):
      print(f"{i}. {city}")

if len(df) >= 6:
      print("\n6th city in the list:", df.iloc[5]['customer_city'])
else:
      print("\nThere are less than 6 cities in the list.")

#Count the number of orders placed in 2017
query = """SELECT COUNT(order_id) FROM orders WHERE YEAR(order_purchase_timestamp) = 2017"""
cur.execute(query)
data = cur.fetchall()
print("Total orders placed in 2017 are ",data)

#Find the total sales per category
query = """SELECT products.product_category AS CATEGORY, ROUND(SUM(payments.payment_value),2) AS SALES
FROM products JOIN order_items
ON products.product_id=order_items.product_id
JOIN payments
ON payments.order_id=order_items.order_id
GROUP BY category"""
cur.execute(query)
data = cur.fetchall()
for row in data:
      print("Category:", row[0], "| Total Sales:", row[1])
df = pd.DataFrame(data)
df.columns = ['Category', 'Sales']
print(df)

#Calculate the percentage of orders that were paid in installments
query = """SELECT (sum(case when payment_installments >= 1 then 1
else 0 end))/count(*) * 100 FROM payments
"""
cur.execute(query)
data = cur.fetchall()
print('The percentage of order that were paid in installments is ',data)

#Count the number of customers from each state
query = """Select customer_state, count(customer_id)
from customers group by customer_state  
 """
cur.execute(query)
data = cur.fetchall()
for row in data:
      print("Customer State:", row[0], "| Count:", row[1])
df = pd.DataFrame(data)
df.columns = ['Customer State', 'Count']
df = df.sort_values(by='Count', ascending=False)
print(df.head())
plt.figure()
plt.bar(df['Customer State'], df['Count'])
plt.xticks(rotation=90)
plt.show()

#Calculate the number of orders per month in 2018.
query = """
SELECT MONTH(order_purchase_timestamp) AS month_number,
       MONTHNAME(order_purchase_timestamp) AS month,
       COUNT(order_id) AS order_count
FROM orders
WHERE YEAR(order_purchase_timestamp) = 2018
GROUP BY MONTH(order_purchase_timestamp),
         MONTHNAME(order_purchase_timestamp)
ORDER BY month_number;
"""
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data)
df.columns = ['Month_Number', 'Month', 'Count']
df = df.sort_values(by='Month_Number', ascending=True)
plt.figure()
colors = np.random.rand(len(df), 3)
bars = plt.bar(df['Month'], df['Count'], color=colors)
plt.xticks(rotation=45)
for bar in bars:
      height = bar.get_height()
      plt.text(bar.get_x() + bar.get_width()/2, height, str(height), ha='center', va='bottom')
plt.show()

#Find the average number of products per order, grouped by customer city
query = """
WITH count_per_order AS
(SELECT orders.order_id, orders.customer_id, COUNT(order_items.order_id) AS oc
FROM orders JOIN order_items
ON orders.order_id = order_items.order_id
GROUP BY orders.order_id, orders.customer_id)

SELECT customers.customer_city, round(avg(count_per_order.oc),2) average_orders
FROM customers JOIN count_per_order
ON customers.customer_id = count_per_order.customer_id
GROUP BY customers.customer_city 
ORDER BY average_orders DESC
"""
cur.execute(query)
data = cur.fetchall()
df=pd.DataFrame(data)
df.columns = 'customer city','average products per order'
print(df.head(10))

#Calculate the percentage of total revenue contributed by each product category.
query = """
SELECT 
    p.product_category AS category, 
    ROUND(
        SUM(pay.payment_value) / 
        (SELECT SUM(payment_value) FROM payments) * 100, 2) AS sales_percentage
FROM products p
JOIN order_items oi
    ON p.product_id = oi.product_id
JOIN payments pay
    ON pay.order_id = oi.order_id
GROUP BY p.product_category
ORDER BY sales_percentage DESC;
"""
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data, columns=['Category', 'Percentage Distribution'])
plt.figure()
plt.pie(df["Percentage Distribution"], labels=df["Category"], autopct='%1.1f%%')
plt.title("Sales Percentage Distribution by Category")
plt.show()

query = """
SELECT products.product_category,
COUNT(order_items.product_id),
ROUND(AVG(order_items.price),2)
FROM products JOIN order_items
ON products.product_id = order_items.product_id
GROUP BY products.product_category;
"""
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data, columns=['Category', 'Order_count', 'Price'])
print(df.head())

#Identify the correlation between product price and the number of times a product has been purchased
arr1 = df['Order_count']
arr2 = df['Price']
print('The correlation is ',np.corrcoef(arr1, arr2))

#Calculate the total revenue generated by each seller, and rank them by revenue
query = '''SELECT
seller_id,
revenue,
DENSE_RANK() OVER (ORDER BY revenue DESC) AS rn
FROM (SELECT oi.seller_id, SUM(pay.payment_value) AS revenue
FROM order_items oi JOIN payments pay
ON oi.order_id = pay.order_id
GROUP BY oi.seller_id
) AS a;'''
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data, columns=['seller_id', 'revenue', 'rank'])
print(df.head())
top5 = df.sort_values(by='revenue', ascending=False).head(5)
plt.figure(figsize=(8,8))
plt.pie(top5['revenue'],labels=top5['seller_id'],autopct='%1.1f%%')
plt.title("Top 5 Sellers Revenue Distribution")
plt.axis('equal')
plt.show()

#Calculate the moving average of order values for each customer over their order history.
query = '''SELECT customer_id, order_purchase_timestamp,
AVG(payment) OVER(PARTITION BY customer_id ORDER BY order_purchase_timestamp
ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mov_ag
FROM
(SELECT orders.customer_id, orders.order_purchase_timestamp,
payments.payment_value as payment
FROM payments JOIN orders
ON payments.order_id = orders.order_id) AS A;
'''
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data, columns = ['Customer_ID', 'Date', 'Moving_Average'])
print(df.head())

#Calculate the cumulative sales per month for each year
query = '''
SELECT years, months, payment, SUM(payment) OVER (ORDER BY years, months) AS cumulative_sales
FROM (
    SELECT YEAR(o.order_purchase_timestamp) AS years,
        MONTH(o.order_purchase_timestamp) AS months,
        ROUND(SUM(p.payment_value), 2) AS payment
    FROM orders o JOIN payments p
        ON o.order_id = p.order_id
    GROUP BY 
        YEAR(o.order_purchase_timestamp),
        MONTH(o.order_purchase_timestamp)
) AS monthly_data;
'''
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data)
print(df.head())

#Calculate the year-over-year growth rate of total sales
query = """
WITH a AS (
    SELECT YEAR(o.order_purchase_timestamp) AS years, ROUND(SUM(p.payment_value), 2) AS payment
    FROM orders o JOIN payments p
        ON o.order_id = p.order_id
    GROUP BY years)
SELECT years, payment, LAG(payment) OVER (ORDER BY years) AS previous_year,
    ROUND(((payment - LAG(payment) OVER (ORDER BY years)) / LAG(payment) OVER (ORDER BY years)) * 100, 2) AS yoy_growth_percent
FROM a; 
"""
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data, columns=['years', 'payment', 'previous_year', 'yoy_growth_percent'])
print(df.head())

#Calculate the retention rate of customers, defined as the percentage of customers who make another purchase
# within 18 months of their first purchase
query = '''
With a as(SELECT customers.customer_id, min(order_purchase_timestamp) first_order
FROM customers JOIN orders
ON customers.customer_id = orders.customer_id
GROUP BY customers.customer_id),
b as (SELECT a.customer_id, COUNT(DISTINCT  orders.order_purchase_timestamp) next_order
FROM a JOIN orders
ON orders.customer_id = a.customer_id
AND orders.order_purchase_timestamp > first_order
AND orders.order_purchase_timestamp < date_add(first_order, interval 18 month)
GROUP BY a.customer_id)
SELECT 100 * COUNT(DISTINCT b.customer_id) / COUNT(DISTINCT a.customer_id)
FROM a left JOIN b 
ON a.customer_id = b.customer_id;
'''
cur.execute(query)
data = cur.fetchall()
print(data)

#Identify the top 3 customers who spent the most money in each year
query = '''
SELECT years, customer_id, payment, d_rank
FROM (
    SELECT YEAR(o.order_purchase_timestamp) AS years, o.customer_id, SUM(p.payment_value) AS payment,
        DENSE_RANK() 
        OVER (PARTITION BY YEAR(o.order_purchase_timestamp)
            ORDER BY SUM(p.payment_value) DESC) AS d_rank
    FROM orders o JOIN payments p
        ON p.order_id = o.order_id
    GROUP BY YEAR(o.order_purchase_timestamp), o.customer_id) AS a 
WHERE d_rank <= 3;
'''
cur.execute(query)
data = cur.fetchall()
df = pd.DataFrame(data, columns=['Years', 'Customer_ID', 'Payment', 'Rank'])
sns.barplot(x='Customer_ID',y='Payment',hue='Years',data=df)
plt.xticks(rotation=90)
plt.show()
