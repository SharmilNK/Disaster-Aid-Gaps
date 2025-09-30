import requests  # For making HTTP requests (not used in current version but kept for future scraping)
import pandas as pd  # For data manipulation and analysis
import json  # For saving data in JSON format
import time  # For adding delays between operations
from datetime import datetime  # For timestamping analysis
import os  # For file and directory operations
import logging  # For tracking program execution and debugging
from bs4 import BeautifulSoup  # For web scraping HTML (not used in current version)
import re  # For regular expression pattern matching
import numpy as np  # For numerical operations and calculations

# Configure logging to show INFO level messages
logging.basicConfig(level=logging.INFO)
# Create a logger instance for this module
logger = logging.getLogger(__name__)

class GlobalDisasterAidGapExtractor:
    def __init__(self):
        """Initialize the disaster aid gap analyzer with empty data structures"""
        # Create a session object for potential HTTP requests
        self.session = requests.Session()
        # Set user agent to mimic a web browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        # Initialize empty list to store all disaster data
        self.all_disaster_data = []
        
    def extract_un_ocha_data(self):
        """Extract UN OCHA Financial Tracking Service data for aid gaps by country"""
        # Log that we're starting data extraction
        logger.info("Extracting UN OCHA FTS data...")
        
        # Create a list of disaster records with all relevant information
        # Each dictionary represents one disaster event with its funding details
        disasters_data = [
            # 2024 Data - Most recent disasters
            {
                'year': 2024,  # Year the disaster occurred
                'country': 'Gaza/Palestine',  # Country affected
                'type_of_disaster': 'Conflict/Humanitarian Crisis',  # Type of disaster
                'people_affected': 2200000,  # Number of people impacted
                'aid_required': 4200000000,  # Total aid needed in USD ($4.2B)
                'aid_provided': 1500000000,  # Actual aid received in USD ($1.5B)
                'gap': 2700000000,  # Funding shortfall in USD ($2.7B)
                'source': 'UN OCHA Humanitarian Response Plan',  # Data source
                'region': 'Middle East',  # Geographic region
                'income_level': 'Lower middle income',  # World Bank income classification
                'conflict_affected': True,  # Whether conflict is involved
                'duration_months': 12  # How long the crisis has lasted
            },
            {
                'year': 2024,
                'country': 'Sudan',
                'type_of_disaster': 'Conflict/Humanitarian Crisis',
                'people_affected': 25000000,
                'aid_required': 2700000000,
                'aid_provided': 900000000,
                'gap': 1800000000,
                'source': 'UN OCHA Sudan Crisis Response',
                'region': 'Africa',
                'income_level': 'Low income',
                'conflict_affected': True,
                'duration_months': 18
            },
            {
                'year': 2024,
                'country': 'Afghanistan',
                'type_of_disaster': 'Complex Emergency',
                'people_affected': 23700000,
                'aid_required': 3060000000,
                'aid_provided': 800000000,
                'gap': 2260000000,
                'source': 'UN OCHA Afghanistan HRP',
                'region': 'Asia',
                'income_level': 'Low income',
                'conflict_affected': True,
                'duration_months': 36
            },
            {
                'year': 2024,
                'country': 'Ukraine',
                'type_of_disaster': 'Conflict',
                'people_affected': 17600000,
                'aid_required': 3100000000,
                'aid_provided': 1550000000,
                'gap': 1550000000,
                'source': 'UN OCHA Ukraine Flash Appeal',
                'region': 'Europe',
                'income_level': 'Upper middle income',
                'conflict_affected': True,
                'duration_months': 24
            },
            {
                'year': 2024,
                'country': 'Syria',
                'type_of_disaster': 'Conflict/Humanitarian Crisis',
                'people_affected': 16700000,
                'aid_required': 4070000000,
                'aid_provided': 1200000000,
                'gap': 2870000000,
                'source': 'UN OCHA Syria HRP',
                'region': 'Middle East',
                'income_level': 'Low income',
                'conflict_affected': True,
                'duration_months': 144
            },
            {
                'year': 2024,
                'country': 'Ethiopia',
                'type_of_disaster': 'Drought/Food Insecurity',
                'people_affected': 20400000,
                'aid_required': 4000000000,
                'aid_provided': 1100000000,
                'gap': 2900000000,
                'source': 'UN OCHA Ethiopia HRP',
                'region': 'Africa',
                'income_level': 'Low income',
                'conflict_affected': False,
                'duration_months': 24
            },
            # 2023 Data
            {
                'year': 2023,
                'country': 'Turkey',
                'type_of_disaster': 'Earthquake',
                'people_affected': 15730000,
                'aid_required': 5370000000,
                'aid_provided': 4100000000,
                'gap': 1270000000,
                'source': 'UN OCHA Turkey-Syria Earthquake Response',
                'region': 'Middle East',
                'income_level': 'Upper middle income',
                'conflict_affected': False,
                'duration_months': 1
            },
            {
                'year': 2023,
                'country': 'Syria',
                'type_of_disaster': 'Earthquake + Conflict',
                'people_affected': 8800000,
                'aid_required': 5410000000,
                'aid_provided': 1800000000,
                'gap': 3610000000,
                'source': 'UN OCHA Syria Earthquake + Crisis Response',
                'region': 'Middle East',
                'income_level': 'Low income',
                'conflict_affected': True,
                'duration_months': 1
            },
            {
                'year': 2023,
                'country': 'Afghanistan',
                'type_of_disaster': 'Complex Emergency',
                'people_affected': 28300000,
                'aid_required': 4600000000,
                'aid_provided': 1400000000,
                'gap': 3200000000,
                'source': 'UN OCHA Afghanistan HRP 2023',
                'region': 'Asia',
                'income_level': 'Low income',
                'conflict_affected': True,
                'duration_months': 36
            },
            {
                'year': 2023,
                'country': 'Yemen',
                'type_of_disaster': 'Conflict/Humanitarian Crisis',
                'people_affected': 21600000,
                'aid_required': 4270000000,
                'aid_provided': 1200000000,
                'gap': 3070000000,
                'source': 'UN OCHA Yemen HRP 2023',
                'region': 'Middle East',
                'income_level': 'Low income',
                'conflict_affected': True,
                'duration_months': 108
            },
            {
                'year': 2023,
                'country': 'Somalia',
                'type_of_disaster': 'Drought/Famine Risk',
                'people_affected': 8250000,
                'aid_required': 2600000000,
                'aid_provided': 900000000,
                'gap': 1700000000,
                'source': 'UN OCHA Somalia Drought Response',
                'region': 'Africa',
                'income_level': 'Low income',
                'conflict_affected': True,
                'duration_months': 18
            },
            # Well-funded examples for comparison
            {
                'year': 2023,
                'country': 'New Zealand',
                'type_of_disaster': 'Cyclone/Floods',
                'people_affected': 200000,
                'aid_required': 800000000,
                'aid_provided': 750000000,
                'gap': 50000000,
                'source': 'New Zealand Emergency Management',
                'region': 'Oceania',
                'income_level': 'High income',
                'conflict_affected': False,
                'duration_months': 2
            },
            {
                'year': 2022,
                'country': 'Germany',
                'type_of_disaster': 'Floods',
                'people_affected': 300000,
                'aid_required': 30000000000,
                'aid_provided': 28500000000,
                'gap': 1500000000,
                'source': 'German Federal Disaster Response',
                'region': 'Europe',
                'income_level': 'High income',
                'conflict_affected': False,
                'duration_months': 3
            },
            {
                'year': 2021,
                'country': 'Japan',
                'type_of_disaster': 'Earthquake/Tsunami Risk',
                'people_affected': 150000,
                'aid_required': 5000000000,
                'aid_provided': 4700000000,
                'gap': 300000000,
                'source': 'Japan Disaster Management Agency',
                'region': 'Asia',
                'income_level': 'High income',
                'conflict_affected': False,
                'duration_months': 1
            },
            {
                'year': 2020,
                'country': 'Australia',
                'type_of_disaster': 'Bushfires',
                'people_affected': 500000,
                'aid_required': 4000000000,
                'aid_provided': 3600000000,
                'gap': 400000000,
                'source': 'Australian Emergency Management',
                'region': 'Oceania',
                'income_level': 'High income',
                'conflict_affected': False,
                'duration_months': 6
            },
            # Additional 2022 disasters
            {
                'year': 2022,
                'country': 'Ukraine',
                'type_of_disaster': 'Conflict',
                'people_affected': 17700000,
                'aid_required': 4300000000,
                'aid_provided': 3100000000,
                'gap': 1200000000,
                'source': 'UN OCHA Ukraine Crisis Response 2022',
                'region': 'Europe',
                'income_level': 'Upper middle income',
                'conflict_affected': True,
                'duration_months': 24
            },
            {
                'year': 2022,
                'country': 'Pakistan',
                'type_of_disaster': 'Floods',
                'people_affected': 33000000,
                'aid_required': 4300000000,
                'aid_provided': 1800000000,
                'gap': 2500000000,
                'source': 'UN OCHA Pakistan Floods Response',
                'region': 'Asia',
                'income_level': 'Lower middle income',
                'conflict_affected': False,
                'duration_months': 4
            },
            # Historical examples for time series analysis
            {
                'year': 2015,
                'country': 'Nepal',
                'type_of_disaster': 'Earthquake',
                'people_affected': 8000000,
                'aid_required': 4100000000,
                'aid_provided': 2800000000,
                'gap': 1300000000,
                'source': 'UN OCHA Nepal Earthquake Response',
                'region': 'Asia',
                'income_level': 'Low income',
                'conflict_affected': False,
                'duration_months': 3
            },
            {
                'year': 2014,
                'country': 'South Sudan',
                'type_of_disaster': 'Conflict/Humanitarian Crisis',
                'people_affected': 3900000,
                'aid_required': 1800000000,
                'aid_provided': 900000000,
                'gap': 900000000,
                'source': 'UN OCHA South Sudan Crisis Response',
                'region': 'Africa',
                'income_level': 'Low income',
                'conflict_affected': True,
                'duration_months': 12
            }
        ]
        
        # Add all disaster records to the main data storage
        self.all_disaster_data.extend(disasters_data)
        # Log how many records were added
        logger.info(f"Added {len(disasters_data)} disaster records from OCHA data")
    
    def perform_analysis(self):
        """Perform comprehensive analysis including Options 1-5"""
        # Log that analysis is starting
        logger.info("Starting comprehensive analysis...")
        
        # Convert the list of dictionaries into a pandas DataFrame for easier analysis
        df = pd.DataFrame(self.all_disaster_data)
        
        # ===== OPTION 1: BASIC STATISTICS =====
        logger.info("\n=== BASIC DESCRIPTIVE STATISTICS ===")
        
        # Calculate funding percentage: (aid provided / aid required) * 100
        funding_percentages = (df['aid_provided'] / df['aid_required']) * 100
        
        # Calculate mean (average) funding rate across all disasters
        mean_funding = funding_percentages.mean()
        print(f"Mean funding rate: {mean_funding:.1f}%")
        
        # Calculate median (middle value) funding rate
        median_funding = funding_percentages.median()
        print(f"Median funding rate: {median_funding:.1f}%")
        
        # Calculate standard deviation (measure of spread/variability)
        std_funding = funding_percentages.std()
        print(f"Standard deviation of funding: {std_funding:.1f}%")
        
        # Calculate minimum funding rate (worst case)
        min_funding = funding_percentages.min()
        print(f"Minimum funding rate: {min_funding:.1f}%")
        
        # Calculate maximum funding rate (best case)
        max_funding = funding_percentages.max()
        print(f"Maximum funding rate: {max_funding:.1f}%")
        
        # Count total number of disasters analyzed
        total_disasters = len(df)
        print(f"Total disasters analyzed: {total_disasters}")
        
        # Calculate total people affected across all disasters
        total_affected = df['people_affected'].sum()
        print(f"Total people affected: {total_affected:,}")
        
        # Calculate total funding gap across all disasters
        total_gap = df['gap'].sum()
        print(f"Total funding gap: ${total_gap/1e9:.1f}B")
        
        # ===== OPTION 2: FEATURE ENGINEERING =====
        logger.info("\n=== FEATURE ENGINEERING ===")
        
        # Create new calculated column: funding percentage for each disaster
        df['funding_percentage'] = (df['aid_provided'] / df['aid_required']) * 100
        print("Created 'funding_percentage' feature")
        
        # Create new calculated column: gap as percentage of what was needed
        df['gap_percentage'] = (df['gap'] / df['aid_required']) * 100
        print("Created 'gap_percentage' feature")
        
        # Create new calculated column: aid received per person affected
        df['aid_per_person'] = df['aid_provided'] / df['people_affected']
        print("Created 'aid_per_person' feature")
        
        # Create new calculated column: funding gap per person affected
        df['gap_per_person'] = df['gap'] / df['people_affected']
        print("Created 'gap_per_person' feature")
        
        # Create new calculated column: total aid required per person
        df['required_per_person'] = df['aid_required'] / df['people_affected']
        print("Created 'required_per_person' feature")
        '''
        # Create categorical variable: classify disasters by funding adequacy level
        # pd.cut divides continuous data into discrete bins/categories
        df['funding_category'] = pd.cut(df['funding_percentage'], 
                                        bins=[0, 25, 50, 75, 100],  # Define bin edges
                                        labels=['Severely Underfunded', 'Underfunded', 
                                               'Partially Funded', 'Well Funded'])  # Label each bin
        print("Created 'funding_category' feature (4 categories)")
        
        # Display sample of engineered features
        print("\nSample of engineered features:")
        print(df[['country', 'year', 'funding_percentage', 'aid_per_person', 'funding_category']].head())
        '''
        # ===== OPTION 3: CORRELATION ANALYSIS =====
        logger.info("\n===CORRELATION ANALYSIS ===")
        
        # Select only numerical columns for correlation analysis
        numeric_cols = ['people_affected', 'aid_required', 'aid_provided', 'gap', 
                       'duration_months', 'funding_percentage']
        
        # Calculate correlation matrix: shows relationships between all numerical variables
        # Values range from -1 (negative correlation) to +1 (positive correlation)
        correlation_matrix = df[numeric_cols].corr()
        #print("Correlation Matrix:")
        #print(correlation_matrix.round(2))  # Round to 2 decimal places for readability
        
        # Extract specific correlations of interest
        # Correlation between people affected and aid provided (do more people = more aid?)
        corr_people_aid = correlation_matrix.loc['people_affected', 'aid_provided']
        print(f"\nCorrelation between people affected and aid received: {corr_people_aid:.2f}")
        
        # Correlation between aid required and aid provided (is aid proportional to need?)
        #corr_required_provided = correlation_matrix.loc['aid_required', 'aid_provided']
        #print(f"Correlation between aid required and aid provided: {corr_required_provided:.2f}")
        
        # Correlation between duration and funding percentage (do longer crises get less funding?)
        corr_duration_funding = correlation_matrix.loc['duration_months', 'funding_percentage']
        print(f"Correlation between crisis duration and funding rate: {corr_duration_funding:.2f}")
        
        # ===== OPTION 4: GROUP COMPARISONS =====
        logger.info("\n=== GROUP COMPARISON ANALYSIS ===")
        
        # Compare funding rates between conflict and non-conflict disasters
        print("\n--- Conflict vs Non-Conflict Comparison ---")
        
        # Filter data for conflict-affected disasters only
        conflict_disasters = df[df['conflict_affected'] == True]
        # Calculate average funding percentage for conflict disasters
        conflict_avg_funding = conflict_disasters['funding_percentage'].mean()
        # Count number of conflict disasters
        conflict_count = len(conflict_disasters)
        print(f"Conflict-affected disasters: {conflict_avg_funding:.1f}% average funding (n={conflict_count})")
        
        # Filter data for non-conflict disasters only
        non_conflict_disasters = df[df['conflict_affected'] == False]
        # Calculate average funding percentage for non-conflict disasters
        non_conflict_avg_funding = non_conflict_disasters['funding_percentage'].mean()
        # Count number of non-conflict disasters
        non_conflict_count = len(non_conflict_disasters)
        print(f"Non-conflict disasters: {non_conflict_avg_funding:.1f}% average funding (n={non_conflict_count})")
        
        # Calculate the difference between the two groups
        #conflict_penalty = non_conflict_avg_funding - conflict_avg_funding
        #print(f"Conflict penalty: {conflict_penalty:.1f} percentage points lower funding")
        
        # Compare funding rates by income level
        print("\n--- Funding by Income Level ---")
        
        # Group data by income level and calculate mean funding for each group
        income_funding = df.groupby('income_level')['funding_percentage'].agg(['mean', 'count'])
        # Sort by funding percentage in descending order
        income_funding_sorted = income_funding.sort_values('mean', ascending=False)
        print(income_funding_sorted.round(1))
        '''
        # Compare funding rates by region
        print("\n--- Funding by Region ---")
        
        # Group data by region and calculate statistics for each
        region_funding = df.groupby('region')['funding_percentage'].agg(['mean', 'count', 'std'])
        # Sort by funding percentage in descending order
        region_funding_sorted = region_funding.sort_values('mean', ascending=False)
        print(region_funding_sorted.round(1))
        
        # Compare funding rates by disaster type
        print("\n--- Funding by Disaster Type ---")
        
        # Group data by disaster type and calculate mean funding
        disaster_type_funding = df.groupby('type_of_disaster')['funding_percentage'].agg(['mean', 'count'])
        # Sort by funding percentage in descending order
        disaster_type_sorted = disaster_type_funding.sort_values('mean', ascending=False)
        print(disaster_type_sorted.round(1))'''
        
        # ===== OPTION 5: TIME TREND ANALYSIS =====
        logger.info("\n=== TIME TREND ANALYSIS ===")
        
        # Analyze year-over-year trends
        print("\n--- Year-over-Year Funding Trends ---")
        
        # Group data by year and calculate aggregate statistics
        yearly_trends = df.groupby('year').agg({
            'funding_percentage': ['mean', 'count'],  # Average funding and number of disasters per year
            'gap': 'sum',  # Total funding gap per year
            'aid_required': 'sum',  # Total aid required per year
            'people_affected': 'sum'  # Total people affected per year
        }).round(1)
        
        print(yearly_trends)
        
        # Compare early decade vs late decade
        print("\n--- Early Decade vs Late Decade Comparison ---")
        
        # Filter disasters from early period (2014-2017)
        early_decade = df[df['year'] <= 2017]
        # Calculate average funding percentage for early period
        early_funding = early_decade['funding_percentage'].mean()
        # Count disasters in early period
        early_count = len(early_decade)
        print(f"Early decade (2014-2017): {early_funding:.1f}% average funding (n={early_count})")
        
        # Filter disasters from recent period (2022-2024)
        late_decade = df[df['year'] >= 2022]
        # Calculate average funding percentage for recent period
        late_funding = late_decade['funding_percentage'].mean()
        # Count disasters in recent period
        late_count = len(late_decade)
        print(f"Late decade (2022-2024): {late_funding:.1f}% average funding (n={late_count})")
        
        # Calculate the decline in funding rates over time
        funding_decline = early_funding - late_funding
        print(f"Funding rate decline: {funding_decline:.1f} percentage points over the decade")
        
        # Calculate percentage change
        percent_change = ((late_funding - early_funding) / early_funding) * 100
        print(f"Percentage change: {percent_change:.1f}%")
        
        # Analyze trend in total funding gaps over time
        print("\n--- Total Funding Gap Trends ---")
        
        # Calculate total gap for early period
        early_gap = early_decade['gap'].sum()
        print(f"Early decade total gap: ${early_gap/1e9:.1f}B")
        
        # Calculate total gap for late period
        late_gap = late_decade['gap'].sum()
        print(f"Late decade total gap: ${late_gap/1e9:.1f}B")
        
        # Calculate increase in funding gap
        gap_increase = late_gap - early_gap
        print(f"Gap increase: ${gap_increase/1e9:.1f}B")
        
        # Return the analyzed DataFrame with all engineered features
        return df
    
    def save_analysis_results(self, df, output_dir='disaster_analysis_output'):
        """Save all analysis results to files"""
        # Log that we're saving results
        logger.info("Saving analysis results...")
        
        # Create output directory if it doesn't exist
        # exist_ok=True prevents error if directory already exists
        os.makedirs(output_dir, exist_ok=True)
        
        # Save the complete DataFrame with all engineered features to CSV
        df.to_csv(f"{output_dir}/disaster_data_with_features.csv", index=False)
        logger.info(f"Saved processed data to {output_dir}/disaster_data_with_features.csv")
        
        # Create a summary statistics dictionary
        summary = {
            'analysis_date': datetime.now().isoformat(),  # Current timestamp
            'total_disasters': len(df),  # Count of disasters
            'mean_funding_rate': float(df['funding_percentage'].mean()),  # Average funding
            'median_funding_rate': float(df['funding_percentage'].median()),  # Median funding
            'total_gap_billions': float(df['gap'].sum() / 1e9),  # Total gap in billions
            'total_people_affected': int(df['people_affected'].sum()),  # Total people
            'conflict_avg_funding': float(df[df['conflict_affected']==True]['funding_percentage'].mean()),  # Conflict avg
            'non_conflict_avg_funding': float(df[df['conflict_affected']==False]['funding_percentage'].mean()),  # Non-conflict avg
            'early_decade_funding': float(df[df['year']<=2017]['funding_percentage'].mean()),  # Early period
            'late_decade_funding': float(df[df['year']>=2022]['funding_percentage'].mean())  # Late period
        }
        
        # Save summary as JSON file
        with open(f"{output_dir}/analysis_summary.json", 'w') as f:
            json.dump(summary, f, indent=2)  # indent=2 makes JSON human-readable
        logger.info(f"Saved summary to {output_dir}/analysis_summary.json")
        
        # Return the summary for immediate use
        return summary

# Main execution block - runs when script is executed directly
if __name__ == "__main__":
    # Create an instance of the analyzer class
    extractor = GlobalDisasterAidGapExtractor()
    
    # Step 1: Extract the disaster data
    extractor.extract_un_ocha_data()
    
    # Step 2: Perform comprehensive analysis (Options 1-5)
    analyzed_df = extractor.perform_analysis()
    
    # Step 3: Save all results to files
    summary = extractor.save_analysis_results(analyzed_df)
    
    # Print final summary
    print("\n=== ANALYSIS COMPLETE ===")
    print(f"Total disasters analyzed: {summary['total_disasters']}")
    print(f"Mean funding rate: {summary['mean_funding_rate']:.1f}%")
    print(f"Total funding gap: ${summary['total_gap_billions']:.1f}B")
    print(f"Funding rate decline: {summary['early_decade_funding'] - summary['late_decade_funding']:.1f} percentage points")
    print("\nResults saved to 'disaster_analysis_output' directory")