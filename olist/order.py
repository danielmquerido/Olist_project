import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist


class Order:
    '''
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    '''
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        # Creating a df with all orders
        orders = self.data['orders'].copy()

        # filter delivered orders
        if is_delivered:
            orders = orders.query("order_status=='delivered'").copy()

        # Handling datetime
        orders['order_purchase_timestamp'] = \
            pd.to_datetime(orders['order_purchase_timestamp'])
        orders['order_approved_at'] = \
            pd.to_datetime(orders['order_approved_at'])
        orders['order_delivered_carrier_date'] = \
            pd.to_datetime(orders['order_delivered_carrier_date'])
        orders['order_delivered_customer_date'] = \
            pd.to_datetime(orders['order_delivered_customer_date'])
        orders['order_estimated_delivery_date'] = \
            pd.to_datetime(orders['order_estimated_delivery_date'])

        # Compute just the number of days in each time_delta
        one_day_delta = np.timedelta64(24, 'h') # a "timedelta64" object of 1 day (24 hours)

        # Assign compute delay vs expected
        orders.loc[:,'wait_time'] = \
            (orders['order_delivered_customer_date'] - orders['order_purchase_timestamp']) / one_day_delta

        orders.loc[:,'delay_vs_expected'] = \
            (orders['order_delivered_customer_date'] - orders['order_estimated_delivery_date']) / one_day_delta

        orders.loc[:,'expected_wait_time'] = \
            (orders['order_estimated_delivery_date'] - orders['order_purchase_timestamp']) / one_day_delta

        # We only want to keep delay where wait_time is longer than expected (not the other way around)
        # This is what drives customer dissatisfaction!
        def handle_delay(x):
            if x > 0:
                return x
            else:
                return 0

        orders.loc[:, 'delay_vs_expected'] = \
            orders['delay_vs_expected'].apply(handle_delay)

        return orders[[
            'order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
            'order_status'
        ]]


    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """

        # Get a copy of reviews dataframe
        reviews = self.data['order_reviews'].copy()
        assert(reviews.shape == (99224,7))

        # Get dim_is_one_star, dim_is_five_star, review_score
        reviews['dim_is_one_star'] = reviews['review_score'].apply(lambda x: 1 if x == 1 else 0)
        reviews['dim_is_five_star'] = reviews['review_score'].apply(lambda x: 1 if x == 5 else 0)

        # Final dataset
        return reviews[['order_id', 'dim_is_one_star', 'dim_is_five_star', 'review_score']]


    def get_number_products(self):
        """
        Returns a DataFrame with:
        order_id, number_of_products
        """
        # Get a copy of products dataframe
        products = self.data['order_items'].copy()

        # Get number of products
        products = products.groupby('order_id',as_index=False).agg({'order_item_id': 'count'})

        # Redefine column names

        products.columns = ['order_id', 'number_of_products']

        return products


    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        # Get a copy of items dataframe
        items = self.data['order_items'].copy()

        # Get the total number of sellers per order
        items = items.groupby('order_id', as_index=False).agg({'seller_id': 'nunique'})

        # Rename column
        items.columns = ['order_id', 'number_of_sellers']

        return items


    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        # Return a dataframe with grouped by order_id, sum all numeric values, with order_id, price, freight_value
        return self.data['order_items'].groupby('order_id', as_index=False).agg({'price': 'sum', 'freight_value': 'sum'})


    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        # Get 5 different dataframes: orders, order_items, sellers, customers, geolocation
        orders = self.data['orders'].copy()
        order_items = self.data['order_items'].copy()
        sellers = self.data['sellers'].copy()
        customers = self.data['customers'].copy()
        geolocation = self.data['geolocation'].copy()

        # Get only the relevant columnns in each dataframe
        orders = orders[['order_id', 'customer_id']]
        order_items = order_items[['order_id', 'seller_id']]
        sellers = sellers[['seller_id', 'seller_zip_code_prefix']]
        customers = customers[['customer_id', 'customer_zip_code_prefix']]
        geolocation = \
            geolocation[['geolocation_zip_code_prefix', 'geolocation_lat', 'geolocation_lng']]

        # One zip code can be linked to multiple lat/lng coordinates, so merge and give only the first lat/lng
        geolocation = \
            geolocation.groupby('geolocation_zip_code_prefix', as_index=False).first()

        # Create 2 dataframes, one merging customers and geolocation, the other merging sellers and geolocation
        customers_geo = \
            customers.merge(geolocation, how='left', left_on='customer_zip_code_prefix', right_on='geolocation_zip_code_prefix')
        sellers_geo = \
            sellers.merge(geolocation, how='left', left_on='seller_zip_code_prefix', right_on='geolocation_zip_code_prefix')

        # Remove geolocation_zip_code_prefix column and rename geolocation_lat and geolocation_lng
        customers_geo = \
            customers_geo.drop(columns=['geolocation_zip_code_prefix'])
        customers_geo = \
            customers_geo.rename(columns={'geolocation_lat': 'customer_lat', 'geolocation_lng': 'customer_lng'})
        sellers_geo = \
            sellers_geo.drop(columns=['geolocation_zip_code_prefix'])
        sellers_geo = \
            sellers_geo.rename(columns={'geolocation_lat': 'seller_lat', 'geolocation_lng': 'seller_lng'})

        # Merge orders and customers, then orders and order_items, then orders and sellers
        orders = orders.merge(customers, on='customer_id')
        orders = orders.merge(order_items, on='order_id')
        orders = orders.merge(sellers, on='seller_id')

        # Merge orders with customers_geo and sellers_geo
        orders = orders.merge(customers_geo, on='customer_id')\
            .merge(sellers_geo, on='seller_id', suffixes=('_customer', '_seller'))

        # Drop null values
        orders = orders.dropna()

        # Create a new column with the distance between customer and seller
        orders.loc[:, 'distance_seller_customer'] =\
            orders.apply(lambda row:
                haversine_distance(row['seller_lng'],row['seller_lat'],row['customer_lng'],row['customer_lat']),axis=1)

        # Create a dataframe "distance" with only order_id and distance_seller_customer
        distance = orders[['order_id', 'distance_seller_customer']]

        # Groupby order_id and get the mean distance_seller_customer, since an order can have multiple items, and rename the column
        distance = \
            distance.groupby('order_id', as_index=False).agg({'distance_seller_customer':'mean'})

        return distance


    def get_training_data(self,
                          is_delivered=True,
                          with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_products', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        # Call all the functions above and get the dataframes and merge them
        training_set =\
            self.get_wait_time(is_delivered)\
                .merge(
                self.get_review_score(), on='order_id'
            ).merge(
                self.get_number_products(), on='order_id'
            ).merge(
                self.get_number_sellers(), on='order_id'
            ).merge(
                self.get_price_and_freight(), on='order_id'
            )

        # Skip heavy computation of distance_seller_customer unless specified
        if with_distance_seller_customer:
            training_set = training_set.merge(
                self.get_distance_seller_customer(), on='order_id')

        return training_set.dropna()
