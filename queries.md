
### **Aggregations and Summarizations**
1. **Total Fare Amount per Vendor**  
   ```sql
   SELECT VendorID, SUM(fare_amount) AS total_fare
   FROM trips
   GROUP BY VendorID;
   ```

2. **Average Trip Distance by Payment Type**  
   ```sql
   SELECT payment_type, AVG(trip_distance) AS avg_distance
   FROM trips
   GROUP BY payment_type;
   ```

3. **Maximum and Minimum Total Amount Charged by Trip Type**  
   ```sql
   SELECT trip_type, MAX(total_amount) AS max_amount, MIN(total_amount) AS min_amount
   FROM trips
   GROUP BY trip_type;
   ```

4. **Number of Trips per Pickup Location**  
   ```sql
   SELECT PULocationID, COUNT(*) AS total_trips
   FROM trips
   GROUP BY PULocationID;
   ```

5. **Total Congestion Surcharge Collected**  
   ```sql
   SELECT SUM(congestion_surcharge) AS total_congestion_surcharge
   FROM trips;
   ```

---

### **Time-Based Analysis**
1. **Trips Count by Hour of the Day**  
   ```sql
   SELECT HOUR(FROM_UNIXTIME(lpep_pickup_datetime)) AS hour_of_day, COUNT(*) AS total_trips
   FROM trips
   GROUP BY hour_of_day;
   ```

2. **Average Fare Amount by Day of the Week**  
   ```sql
   SELECT DAYOFWEEK(FROM_UNIXTIME(lpep_pickup_datetime)) AS day_of_week, AVG(fare_amount) AS avg_fare
   FROM trips
   GROUP BY day_of_week;
   ```

3. **Trips Starting in Peak Congestion Hours (8 AM to 10 AM)**  
   ```sql
   SELECT COUNT(*) AS peak_hour_trips
   FROM trips
   WHERE HOUR(FROM_UNIXTIME(lpep_pickup_datetime)) BETWEEN 8 AND 10;
   ```

4. **Fare Collected by Week**  
   ```sql
   SELECT WEEK(FROM_UNIXTIME(lpep_pickup_datetime)) AS week_number, SUM(fare_amount) AS total_fare
   FROM trips
   GROUP BY week_number;
   ```

5. **Longest Trip Distance by Month**  
   ```sql
   SELECT MONTH(FROM_UNIXTIME(lpep_pickup_datetime)) AS month, MAX(trip_distance) AS longest_distance
   FROM trips
   GROUP BY month;
   ```

---

### **Drill-Down and Roll-Up**
1. **Total Fare by Vendor and Pickup Location**  
   ```sql
   SELECT VendorID, PULocationID, SUM(fare_amount) AS total_fare
   FROM trips
   GROUP BY ROLLUP(VendorID, PULocationID);
   ```

2. **Average Trip Distance by Vendor and Rate Code**  
   ```sql
   SELECT VendorID, RatecodeID, AVG(trip_distance) AS avg_distance
   FROM trips
   GROUP BY VendorID, RatecodeID;
   ```

3. **Total Congestion Surcharge by Trip Type with Roll-Up**  
   ```sql
   SELECT trip_type, SUM(congestion_surcharge) AS total_surcharge
   FROM trips
   GROUP BY ROLLUP(trip_type);
   ```

4. **Tip Amount Analysis by Payment Type and Vendor**  
   ```sql
   SELECT payment_type, VendorID, SUM(tip_amount) AS total_tip
   FROM trips
   GROUP BY CUBE(payment_type, VendorID);
   ```

5. **Fare Amount Roll-Up by Pickup and Dropoff Location**  
   ```sql
   SELECT PULocationID, DOLocationID, SUM(fare_amount) AS total_fare
   FROM trips
   GROUP BY ROLLUP(PULocationID, DOLocationID);
   ```

---

### **Slicing and Dicing**
1. **Trips with Distance Greater than 5 Miles and Fare Above $20**  
   ```sql
   SELECT *
   FROM trips
   WHERE trip_distance > 5 AND fare_amount > 20;
   ```

2. **Trips with Passenger Count Between 2 and 4**  
   ```sql
   SELECT *
   FROM trips
   WHERE passenger_count BETWEEN 2 AND 4;
   ```

3. **Trips in a Specific Pickup Location with High Tips**  
   ```sql
   SELECT *
   FROM trips
   WHERE PULocationID = 10 AND tip_amount > 10;
   ```

4. **Trips Using Credit Card Payment During Peak Hours**  
   ```sql
   SELECT *
   FROM trips
   WHERE payment_type = 1 AND HOUR(FROM_UNIXTIME(lpep_pickup_datetime)) BETWEEN 17 AND 20;
   ```

5. **Trips with Congestion Surcharge Applied**  
   ```sql
   SELECT *
   FROM trips
   WHERE congestion_surcharge > 0;
   ```

---

### **Ad-Hoc Analysis**
1. **Trips Ending at a Specific Dropoff Location with High Fare Amounts**  
   ```sql
   SELECT *
   FROM trips
   WHERE DOLocationID = 50 AND fare_amount > 50;
   ```

2. **Identify Outliers in Trip Distance (Above 20 Miles)**  
   ```sql
   SELECT *
   FROM trips
   WHERE trip_distance > 20;
   ```

3. **Most Common Pickup Location for Short Trips (< 2 Miles)**  
   ```sql
   SELECT PULocationID, COUNT(*) AS trip_count
   FROM trips
   WHERE trip_distance < 2
   GROUP BY PULocationID
   ORDER BY trip_count DESC;
   ```

4. **Top 3 Vendors by Total Revenue**  
   ```sql
   SELECT VendorID, SUM(total_amount) AS total_revenue
   FROM trips
   GROUP BY VendorID
   ORDER BY total_revenue DESC
   LIMIT 3;
   ```

5. **Trips with Zero Toll Amount but Congestion Surcharge Applied**  
   ```sql
   SELECT *
   FROM trips
   WHERE tolls_amount = 0 AND congestion_surcharge > 0;
   ```

---

### **Complex Joins and Multi-Dimensional Analysis**
1. **Join Trips with Vendor Details**  
   ```sql
   SELECT t.*, v.vendor_name
   FROM trips t
   JOIN vendors v ON t.VendorID = v.id;
   ```

2. **Trips with Matching Location IDs**  
   ```sql
   SELECT t1.*, t2.*
   FROM trips t1
   JOIN trips t2 ON t1.DOLocationID = t2.PULocationID;
   ```

3. **Aggregate Metrics by Trip Type and Payment Method**  
   ```sql
   SELECT trip_type, payment_type, AVG(fare_amount) AS avg_fare, SUM(tip_amount) AS total_tips
   FROM trips
   GROUP BY trip_type, payment_type;
   ```

4. **Total Revenue by Vendor and Day of the Week**  
   ```sql
   SELECT VendorID, DAYOFWEEK(FROM_UNIXTIME(lpep_pickup_datetime)) AS day, SUM(total_amount) AS total_revenue
   FROM trips
   GROUP BY VendorID, day;
   ```

5. **Cross Analysis Between Rate Codes and Payment Types**  
   ```sql
   SELECT RatecodeID, payment_type, COUNT(*) AS total_trips
   FROM trips
   GROUP BY RatecodeID, payment_type;
   ```

---

### **Hierarchical Queries**
1. **Rank Vendors by Total Revenue**  
   ```sql
   SELECT VendorID, SUM(total_amount) AS total_revenue,
          RANK() OVER (ORDER BY SUM(total_amount) DESC) AS rank
   FROM trips
   GROUP BY VendorID;
   ```

2. **Find Top Trip by Distance for Each Pickup Location**  
   ```sql
   SELECT PULocationID, trip_distance,
          ROW_NUMBER() OVER (PARTITION BY PULocationID ORDER BY trip_distance DESC) AS rank
   FROM trips;
   ```

3. **Trips Ranked by Tip Amount**  
   ```sql
   SELECT *, RANK() OVER (ORDER BY tip_amount DESC) AS rank
   FROM trips;
   ```

4. **Find Average Fare and Ranking by Vendor**  
   ```sql
   SELECT VendorID, AVG(fare_amount) AS avg_fare,
          DENSE_RANK() OVER (ORDER BY AVG(fare_amount) DESC) AS rank
   FROM trips
   GROUP BY VendorID;
   ```

5. **Identify Top Payment Type by Revenue for Each Day**  
   ```sql
   SELECT DAY(FROM_UNIXTIME(lpep_pickup_datetime)) AS day, payment_type, SUM(total_amount) AS total_revenue,
          ROW_NUMBER() OVER (PARTITION BY DAY(FROM_UNIXTIME(lpep_pickup_datetime)) ORDER BY SUM(total_amount) DESC) AS rank
   FROM trips
   GROUP BY day, payment_type;
   ```

---

### **Comparison and Benchmarking**
1. **Compare Average Trip Distance for Vendor 1 and Vendor 2**  
   ```sql
   SELECT VendorID, AVG(trip_distance) AS avg_distance
   FROM trips
   WHERE VendorID IN (1, 2)
   GROUP BY VendorID;
   ```

2. **Benchmark Tip Amount by Payment Type**  
   ```sql
   SELECT payment_type, AVG(tip_amount) AS avg_tip
   FROM trips
   GROUP BY payment_type;
   ```

3. **Compare Revenue Between Peak and Off-Peak Hours**  
   ```

sql
   SELECT CASE
            WHEN HOUR(FROM_UNIXTIME(lpep_pickup_datetime)) BETWEEN 7 AND 9 THEN 'Peak'
            ELSE 'Off-Peak'
          END AS time_period, SUM(total_amount) AS total_revenue
   FROM trips
   GROUP BY time_period;
   ```

4. **Compare Trip Count by Weekday and Weekend**  
   ```sql
   SELECT CASE
            WHEN DAYOFWEEK(FROM_UNIXTIME(lpep_pickup_datetime)) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
          END AS day_type, COUNT(*) AS trip_count
   FROM trips
   GROUP BY day_type;
   ```

5. **Analyze Vendor Performance by Total Revenue**  
   ```sql
   SELECT VendorID, SUM(total_amount) AS total_revenue
   FROM trips
   GROUP BY VendorID
   HAVING total_revenue > (
       SELECT AVG(SUM(total_amount))
       FROM trips
       GROUP BY VendorID
   );
   ``` 

These queries cover diverse analytical needs across the specified categories.