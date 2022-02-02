from data_manager import DataManager
import matplotlib.pyplot as plt

if __name__ == "__main__":
    data_manager = DataManager()
    data_manager.create_database()
    data_manager.clean_data()
    data_manager.seed_database('vaccin_covid.csv')
    plot_data = data_manager.get_column_data('Serbia', 'total_vaccinations', 20)
    print(plot_data)
    # plotting the points
    plt.plot(plot_data[0], plot_data[1])
    # naming the x axis
    plt.xlabel('Date')
    plt.xticks(rotation=90)
    # naming the y axis
    plt.ylabel('Total Vaccinations')
    # giving a title to my graph
    plt.title('Vaccination Trend')
    # function to show the plot
    plt.show()