'''Monte Carlo file to generate and run MC simulations'''

from . import supportfunctions as sf
import pandas as pd
import numpy as np
from scipy.stats import truncnorm
import matplotlib.pyplot as plt



class MonteCarloSimulations:

    def __init__(self):
    
        self.day_rates_filepath = sf.get_filepath(
            'database', 'gold', 'base_rates_validity_checked.csv'
                                                 )
        # Read in day rates data
        self.df_day_rates = pd.read_csv(
                        filepath_or_buffer=self.day_rates_filepath)[['company', 'valid_price']]\
                            .sort_values(by = 'valid_price').reset_index(drop=True
                                )
        
        self.mc_data = None
        self.std_day_rates = None

    def prepare_params_for_mc(self):
        """ Using day_rates data produce descriptive stats to run MC then run at the end.
        
        :Returns:
            mc_data (Any): Randomly generated MC data using distribution descriptive stats.
            num_bins (int): Number of bins for histogram plot

        """                
        # Get std of rates
        l_day_rates = self.df_day_rates['valid_price'].to_list()
        self.std_day_rates = np.std(l_day_rates)

        # TODO: shouldnt be hardcoded
        delta = 100
        # Widen upper and lower bound mc value could be
        min_base_mc = np.min(l_day_rates) - delta
        max_base_mc = np.max(l_day_rates) + delta

        #Number of bins for Histogram        
        num_bins = round((max_base_mc - min_base_mc) / self.std_day_rates)

        mean_day_rates = np.mean(l_day_rates)

        # bounds in standard deviations from the mean
        a = (min_base_mc - mean_day_rates) / self.std_day_rates  
        b = (max_base_mc - mean_day_rates) / self.std_day_rates  

        # Fit a truncated normal distribution
        dist = truncnorm(a, b, loc=mean_day_rates, scale=self.std_day_rates)

        # Generate random samples from the distribution
        sample_size = 5000  # specify the number of samples you want to generate
        # Obtain random day rates following distribution of day_rate_data
        mc_data = dist.rvs(sample_size)

        return (mc_data, num_bins)

    def plot_mc_data(self, mc_data, title : str, num_bins : int):
        # Plotting the samples
        plt.figure(figsize=(10, 6))
        bin_width = self.std_day_rates
        plt.hist(mc_data, bins=num_bins, density=True, alpha=0.7, color='blue', edgecolor='black')

        plt.title('Histogram of Samples from Monte Carlos')
        plt.xlabel('day rate / £')
        plt.ylabel('Density')
        plt.grid(True)

        plot_filepath = sf.get_filepath('database', 'gold', 'plots', title)
        plt.savefig(plot_filepath)



if __name__ == '__main__':
    mc = MonteCarloSimulations()
    mc_data, num_bins = mc.prepare_params_for_mc()
    mc.plot_mc_data(title='onshore_follower_day_rate_mc.png', num_bins=num_bins)
