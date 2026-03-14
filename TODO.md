1. Make small examples of triples implemented for Sanjay to follow

2. Look into implementing sanjay's Ui into my own agent flow

3. Implement logic for checking where there are empty parcels of land:
    a. Take the points of all civic addresses
    b. return all the points that are NOT inside the building polygon objects
    c. Make clear the assumption that, for there to be a parcel, there must first be a civic address.
    d. If a given civic address cannot be found in the building parcels, we then know it must be an empty parcel
    e. But we do not know the size of this parcel, as no land info was given (only land info is given for already built-on parcels)