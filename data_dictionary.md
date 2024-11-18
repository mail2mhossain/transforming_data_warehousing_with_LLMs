# Data Dictionary for LPEP Trip (Green Trips) Records



| Field Name             | Description                                                 |
|------------------------|-------------------------------------------------------------------------------------------------------|
| **VendorID**           | Code identifying the LPEP provider. Values: 1 = Creative Mobile Technologies, LLC; 2 = VeriFone Inc. |
| **lpep_pickup_datetime** | Date and time when the meter started.                                                                |
| **lpep_dropoff_datetime** | Date and time when the meter stopped.                                                              |
| **Passenger_count**    | Number of passengers in the vehicle, entered by the driver.                                           |
| **Trip_distance**      | Distance of the trip in miles, as reported by the taximeter.                                          |
| **PULocationID**       | TLC Taxi Zone where the meter was started.                                                            |
| **DOLocationID**       | TLC Taxi Zone where the meter was stopped.                                                            |
| **RateCodeID**         | Code for the final rate used at the trip's end. Options: 1 = Standard rate, 2 = JFK, 3 = Newark, 4 = Nassau or Westchester, 5 = Negotiated fare, 6 = Group ride              
| **Store_and_fwd_flag** | Indicates if the trip record was stored in the vehicle's memory before sending. Y = stored and forwarded trip, N = not a stored and forwarded trip            |
| **Payment_type**       | Code indicating the payment method. Options:  1 = Credit card, 2 = Cash, 3 = No charge, 4 = Dispute , 5 = Unknown, 6 = Voided trip                                                        |
| **Fare_amount**        | Time and distance-based fare calculated by the meter.                                                 |
| **Extra**              | Additional surcharges, including rush hour and overnight charges of $0.50 and $1, respectively.       |
| **MTA_tax**            | $0.50 MTA tax applied automatically based on the metered rate.                                        |
| **Improvement_surcharge** | $0.30 surcharge applied at the start of the trip, introduced in 2015.                             |
| **Tip_amount**         | Tip amount for credit card payments; cash tips are not included.                                      |
| **Tolls_amount**       | Total amount of tolls paid during the trip.                                                           |
| **Total_amount**       | Total charged to passengers, excluding cash tips.                                                     |
| **Trip_type**          | Indicates whether the trip was a street-hail or a dispatch. Options: 1 = Street-hail, 2 = Dispatch                                |
