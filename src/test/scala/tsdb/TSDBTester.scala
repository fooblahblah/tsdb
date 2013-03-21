package tsdb


object TSDBTester extends App {
  val db = new TSDB("/tmp/tsdb.h5")

  val startTime = System.currentTimeMillis()

  1 to 86400 foreach { i =>
    val t = startTime + (i * 1000)
    db.write("stats_counts/cupcake/web_traffic/impression", t, Math.random() * 100)
//    db.write("stats_counts/cupcake/web_traffic/conversion", t, Math.random() * 10)
  }

//  db.close

//
//  val start = System.currentTimeMillis()
//
//  val compounds = writer.compounds()
//  val indexType = compounds.getInferredType(classOf[IndexEntry])
//  val entryType = compounds.getInferredType(classOf[Entry])
//
//  // Create the index if it doesn't exist
//  val index = if(!writer.exists("/index")) writer.createGroup("/index")
//
//  val path    = "/a/impressions"
//  val indexes = if(writer.exists("/index")) writer.getGroupMemberPaths("/index").toList else Nil
//  println(s"indexes: $indexes")
//
//  println(indexes.lastOption.map(writer.tryGetSymbolicLinkTarget(_)))
//
//  if(!writer.exists(path)) writer.createCompoundArray(path, entryType, 0)
//  val dataOffset  = if(writer.exists(path)) writer.getNumberOfElements(path) else 0
//
//  println(s"dataOffset = $dataOffset")
//
//  val data = (dataOffset until dataOffset + 100000) map(Entry(_, Math.random() * 100))
//
//  writer.writeCompoundArrayBlockWithOffset(path, entryType, data.toArray, dataOffset)
//  writer.createSoftLink(s"$path/$dataOffset", s"/index/$dataOffset")
//
//  val checkpoint = System.currentTimeMillis()
//
//  println(s"elapsed write: ${checkpoint - start}")
//
////  writer.flush
//
//  println(writer.getGroupMembers("/a"))
//  println(writer.getNumberOfElements("/a/impressions"))
//  println(writer.exists("/a/conversions"))
////  writer.readCompoundArray("/a/impressions", entryType).foreach(println)
//
//  writer.close
//
//  val reader = HDF5Factory.openForReading("/tmp/tsdb.h5")
//  val compounds2 = reader.compounds()
//  val entryType2 = compounds2.getInferredType(classOf[Entry])
//
//  println("avg = " + reader.readCompoundArrayBlockWithOffset("/a/impressions", entryType2, 10000, 10000).foldLeft(0.0) (_ + _.value)/10000)
//

  println(s"elapsed: ${System.currentTimeMillis() - startTime}")

  println("done!")

  db.stop
}
