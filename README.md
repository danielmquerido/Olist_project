# Olist_project

## The problem
Come up with a solution for the CEO of Olist to help with profit

## The data
9 CSV files with all the information about customers, sellers, purchases, and reviews

## The project

The project consisted of creating functions to get and merge dataframes to get the information I needed.
After the functions were created, came a deep analysis of the information. 
It was a well-known fact that bad reviews were directly related to low profit. 
I tried to figure out what features were more related to the review scores

## The solution

In the end, it was clear that the time waited for delivery, and the difference between the time that was told to the customer (expected wait time) and the actual time it took, were the biggest factors in bad reviews.
I also found that the sellers with more bad reviews, were the ones that sold the most, so excluding them from the website was not an option.
The distance seller-customer was also related to bad reviews since the further a seller is from a customer, the more time the customer has to wait.
In conclusion, my proposal was to stop long deliveries and penalize sellers for having bad reviews, having to pay more to the company, or giving a higher percentage of their sales.
