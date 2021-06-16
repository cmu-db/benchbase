package com.oltpbenchmark.benchmarks.seats.procedures;

import java.util.List;

public class Config {

    private final List<Object[]> configProfile;
    private final List<Object[]> configHistogram;
    private final List<Object[]> countryCodes;
    private final List<Object[]> airportCodes;
    private final List<Object[]> airlineCodes;
    private final List<Object[]> flights;

    public Config(List<Object[]> configProfile, List<Object[]> configHistogram, List<Object[]> countryCodes, List<Object[]> airportCodes, List<Object[]> airlineCodes, List<Object[]> flights) {
        this.configProfile = configProfile;
        this.configHistogram = configHistogram;
        this.countryCodes = countryCodes;
        this.airportCodes = airportCodes;
        this.airlineCodes = airlineCodes;
        this.flights = flights;
    }

    public List<Object[]> getConfigProfile() {
        return configProfile;
    }

    public List<Object[]> getConfigHistogram() {
        return configHistogram;
    }

    public List<Object[]> getCountryCodes() {
        return countryCodes;
    }

    public List<Object[]> getAirportCodes() {
        return airportCodes;
    }

    public List<Object[]> getAirlineCodes() {
        return airlineCodes;
    }

    public List<Object[]> getFlights() {
        return flights;
    }
}
