#!/usr/bin/env groovy
@Grapes(
        @Grab(group = 'org.codehaus.gpars', module = 'gpars', version = '1.2.1')
)
import groovyx.gpars.dataflow.Dataflows

import static groovyx.gpars.dataflow.Dataflow.task

final Dataflows data = new Dataflows()

task {
    println("Task 'convertTemperature' is waiting for dataflow variable 'cityWeather'")


    final cityWeather = data.cityWeather
    final cityTemperature = cityWeather.temperature

    println("Task 'convertTemperature' got dataflow variable 'cityWeather'")

    //Convert the value with a webservice at
    // www.webservicex.net
    final params = [
            Temperature: cityTemperature,
            FromUnit   : 'degreeCelsius',
            ToUnit     : 'degreeFahrenheit'
    ]

    final url = "http://www.webservicex.net/ConvertTemperature.asmx/ConvertTemp"
    final result = downloadData(url, params)

    data.temperature = result.text()
}

//find the temperature for a city
task {
    println "Task 'findCityWeather' is waiting for dataflow 'searchCity'"

    // Get the value for the city attribute in
    // Dataflows data object.  This is
    // set in another task (startSearch)
    // at another time.
    // If the value is not set, then wait.
    final city = data.searchCity

    println "Task 'findCityWeather' got dataflow variable 'searchCity"

    // Get the temperature for the city with
    // the webservice at api.openweathermap.org
    final params = [
            q    : city,
            units: 'metric',
            mode : 'xml'
    ]

    final url = "http://api.openweathermap.org/data/2.5/find"
    final result = downloadData(url, params)
    final temperature = result.list.item.temperature.@value
    assert temperature

    // Assign map value to cityWeather dataflow
    // variable int he Dataflows data Object.
    data.cityWeather = [city: city, temperature: temperature]
}

// Get city part from the search String.
task {
    println "Task 'parseCity' is waiting for dataflow variable 'searchCity'"

    // Get the value for the city attribute in
    // Dataflows data object.  This is set in another task (startSearch)
    // at another time.
    // If the value is not yet available, wait.

    final city = data.searchCity

    println "Task 'parseCity' got dataflow variable 'searchCity'"

    final cityName = city.split(',').first()

    // Assign to the dataflow variable in the dataflows object.
    data.cityName = cityName
}

final startSearch = task {
    // Use the command line argument to set the city dataflow
    // variable  in Dataflows data object.  Any code theat reads
    // this value was waiting, but will nstart now because of this
    // assignment.  How cool is that?
    data.searchCity = args[0]
}

final printValueBound = { dataflowVar, value ->
    println "Variable '$dataflowVar' bound to '$value'"
}

data.searchCity printValueBound.curry('searchCity')
data.cityName printValueBound.curry('cityName')
data.cityWeather printValueBound.curry('cityWeather')
data.temperature printValueBound.curry('temperature')

// Here we read the dataflow variables cityWeather and temperature
// from the dataflows data object.  Notice that once the value is
// set it is not calculated again. For example cityWeather
// will not do a remote call again, because the value is already
// known by now.
println "Main thread is waiting for dataflow variables 'cityWeather', 'temperature' and 'cityName'"

final cityInfo =
        data.cityWeather + [tempFahrenheit: data.temperature] + [cityName: data.cityName]


println """

Temperature in city $cityInfo.cityName (searched with $cityInfo.city):
$cityInfo.temperature Celcius
$cityInfo.tempFahrenheit Fahrenheit
"""

def downloadData(requestUrl, requestParams)
{
    final params = requestParams.collect { it }.join('&')
    final url = "${requestUrl}?${params}"
    final xml = url.toURL().text
    final response = new XmlSlurper().parseText(xml)
    response
}