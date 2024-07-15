"""Class to plot visuals for a given input of prices"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from . import supportfunctions as sf
from scipy.stats import truncnorm

class ComputeStats:
    """For a given rate type (e.g. Junior rates) compute descriptive stats and run monte carlos. Finally plot distribution of rates and monte carlos.
    
    # TODO: In docs describe in more detail about deisions made in the code.
    """

    def __init__(self, l_day_rates : list[float]) -> None:
        
        self.l_day_rates = l_day_rates
        # descriptive stats
        self.std_day_rates = None
        self.mean_day_rates = None
        self.min_day_rate = None
        self.max_day_rate = None
        self.num_bins = None
        self.bins = None
        self.counts = None

        # MC params
        self.min_base_mc = None
        self.max_base_mc = None
        self.num_bins_mc = None
        self.mc_bins = None
        self.mc_counts = None
        
        # MC trials
        self.mc_data = None

        self.kubrick_rate_dict = {
                                    'min' : 350
                                    ,'max' : 1350
                                 }
        
        self.kubrick_rate_dict['delta'] = self.kubrick_rate_dict['max'] - self.kubrick_rate_dict['min']


    def compute_stats_and_plot(self, title : str, rate_type : str) -> None:
        """Calling this method will call the methods compute_desc_stats and plot_histogram."""

        self.compute_desc_stats()
        self.plot_histogram(title=title, rate_type=rate_type)
        self.monte_carlo()
        self.plot_mc_data(title=title)

    def compute_desc_stats(self):
        """Compute descriptive stats to plot visuals and prepare data for MC.
        
        :Params:
            None: Use day rates list available post-instantiation.

        :Returns:
            N/A: Descriptive stats computed and stored as attributes & params for MC prepared.

        """

        # Get std of rates
        self.std_day_rates = np.std(self.l_day_rates)

        # Compute STD and mean
        delta = self.std_day_rates
        self.mean_day_rates = np.mean(self.l_day_rates)
        self.min_day_rate = np.min(self.l_day_rates)
        self.max_day_rate = np.max(self.l_day_rates)

        self.hist_upper_range = self.min_day_rate + self.std_day_rates * \
                                 np.ceil((self.max_day_rate - self.min_day_rate) / self.std_day_rates)
        # Range to be from actual minimum value to above the maximum day rate
        self.histogram_range = [self.min_day_rate, self.hist_upper_range ]

        # Widen upper and lower bound mc value could be
        self.min_base_mc = np.min(self.l_day_rates) - delta
        self.max_base_mc = np.max(self.l_day_rates) + delta

        # Dont want the minimum price to be negative (not realistic pricing scheme)
        if self.min_base_mc < 0:
            self.min_base_mc = 50

        # Number of bins for Histogram        
        self.num_bins = int(np.ceil((self.max_day_rate - self.min_day_rate) / self.std_day_rates))
        # bins for MC (different range)
        self.num_bins_mc = int(np.ceil((self.max_base_mc - self.min_base_mc) / self.std_day_rates))


    def monte_carlo(self):
        """Execute monte carlo
        
        :Params:
            None: Use attribute variables.

        :Returns:
            None: assigns mc data samples to attribute.
        """
        # bounds in standard deviations from the mean
        a = (self.min_base_mc - self.mean_day_rates) / self.std_day_rates  
        b = (self.max_base_mc - self.mean_day_rates) / self.std_day_rates  

        # Fit a truncated normal distribution
        dist = truncnorm(a, b, loc=self.mean_day_rates, scale=self.std_day_rates)

        # Generate random samples from the distribution
        # TODO: make this an option
        sample_size = 5000 
        # Obtain random day rates following distribution of day_rate_data
        self.mc_data = dist.rvs(sample_size)

    def plot_mc_data(self, title: str):
        # Plotting the samples
        plt.figure(figsize=(10, 6))
        counts, bins, _ = plt.hist(self.mc_data, bins=self.num_bins_mc, alpha=0.7, color='blue', edgecolor='black')
        # Calculate total count
        total_count = np.sum(counts)
        # Convert counts to percentages
        percentages = (counts / total_count) * 100
        bins_centred = [(bins[i] + bins[i+1])/2 for i in range(len(bins) - 1)]
        self.mc_bins = bins
        self.mc_counts = counts

        # Plot histogram with percentages on y-axis
        plt.clf()  # Clear previous plot (if any)
        plt.bar(bins_centred, percentages, width=np.diff(bins), edgecolor='black', alpha=0.7, color = 'blue')
        plt.title('MC for ' + title.replace(' distribution', ''))
        plt.xlabel('day rate / £')
        plt.ylabel('Percentage of total')
        plt.grid(True)

        plot_filepath = sf.get_filepath('database', 'gold', 'plots', 'MCs for ' + title)
        plt.savefig(plot_filepath)


    def plot_histogram(self, title : str, rate_type : str):
        """Plot histogram using input data.
        
        Output is to display a histogram plot.

        NOTE: Ultimately output should be to save a png file in plots folder.
        """
        plt.clf()  # Clear previous plot (if any)
        # Create histogram
        counts, bins, _ = plt.hist(self.l_day_rates, bins=self.num_bins
                                   , edgecolor='black', alpha=0.7, color = 'blue'
                                   , range = self.histogram_range 
                                  )
        # Calculate total count
        total_count = np.sum(counts)
        plt.xlim(left  = 0)
        # Convert counts to percentages
        percentages = (counts / total_count) * 100
        bins_centred = [(bins[i] + bins[i+1])/2 for i in range(len(bins) - 1)]
        self.bins = bins
        self.counts = counts

        # Plot histogram with percentages on y-axis
        plt.clf()  # Clear previous plot (if any)
        plt.bar(bins_centred, percentages, width=np.diff(bins), edgecolor='black', alpha=0.7)
        plt.xlabel('Day Rate / £')
        plt.ylabel('Percentage of Total Companies')
        plt.xlim(left  = 0)
        plt.title(title)

        plt.axvline(x=self.kubrick_rate_dict[rate_type], color='r', linestyle='-.', label='Kubrick Group')
        plt.legend()

        plot_filepath = sf.get_filepath('database', 'gold', 'plots', title.replace(' ', '_'))
        plt.savefig(plot_filepath)

    
        
