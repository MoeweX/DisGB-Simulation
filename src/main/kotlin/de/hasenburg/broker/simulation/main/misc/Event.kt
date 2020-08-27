package de.hasenburg.broker.simulation.main.misc

import de.hasenburg.geobroker.commons.model.spatial.Geofence

data class Event(val topic: Topic, val geofence: Geofence)