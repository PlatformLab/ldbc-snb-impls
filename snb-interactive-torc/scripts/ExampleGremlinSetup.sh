import net.ellitron.torc.*
import net.ellitron.torc.util.*
import net.ellitron.ldbcsnbimpls.interactive.core.*
import net.ellitron.ldbcsnbimpls.interactive.torc.*

graph = TorcGraph.open(["gremlin.torc.coordinatorLocator": "basic+udp:host=10.10.1.7,port=12246", "gremlin.torc.graphName": "ldbc_snb_sf0001"])
g = graph.traversal()

torcPersonId = new UInt128(TorcEntity.PERSON.idSpace, 933)

g.V(torcPersonId).valueMap()
