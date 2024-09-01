#Author: Eric Byjoo
#Collaborators: Jack Bresney, Jubrial Saigh
from mrjob.job import MRJob   

#Code given 
class City(MRJob):  # Choose a good class name
    #mapper from debug lab, no lines changed 
    def mapper(self, key, line): # when reading a text file from hdfs, key is None and value is the line of text
        parts = line.split("\t")
        if 'Population' in line: # Added if statment to make sure we don't read the header line
            return
        if len(parts) != 7: # Added if statment so we don't ingest bad data
            return
        city = parts[0]
        state = parts[2]
        population = parts[4]
        zipcodes = parts[5].split(" ") # parts[5] is a list of space separated zip codes
        yield state, (population, len(zipcodes))
    #Combiner identical to reducer from debug lab, used to reduce amount of operations needed when reducing
    def combiner(self, state, values):
        num_cities = 0 # Intialized all variables
        population = 0
        maxzips = 0
        for (p,z) in values:
            num_cities += 1 # Count number of cities
            population += int(p) # update the population, make sure we are adding with ints
            maxzips = max(maxzips, z) # update the maximum number of zip codes in a city in the state.
        yield state, (num_cities, population, maxzips)
    #reducer same as combiner except since this is a total count we add by the value in the tuple instead of just 1
    def reducer(self, state, values):
        num_cities_final = 0 
        population_final = 0
        maxzips_final = 0
        for (num_cities, population, maxzips) in values:
            num_cities_final += num_cities # add value since they've already been counted by key same goes for population
            population_final += int(population)
            maxzips_final = max(maxzips_final,maxzips)         
        yield state, (num_cities_final, population_final, maxzips_final)
if __name__ == '__main__':
    City.run()  # if you don't have these two lines, your code will not do anything 
