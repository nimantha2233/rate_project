'''A python file to process extracted rate card data from a csv file and prepare data to 
be used by stats.py module to output histograms of real distributions and mc distributions
'''
from utils import stats



def main():

    plotter = stats.Plotter()
    plotter.filter_by_location_compute_and_plot()

    return 0



if __name__ == '__main__':
    main()






















