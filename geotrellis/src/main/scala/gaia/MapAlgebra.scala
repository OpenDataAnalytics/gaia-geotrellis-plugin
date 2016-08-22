package gaia

import com.typesafe.config.ConfigFactory
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import java.io.File

object MapAlgebra {

  /**
    * TODO: Take as input a string representation of an equation, along with the usual inputs and output,
    * then run the equation on the inputs.  Scala equivalent of gaia.geo.processes_raster.RasterMathProcess.
    * Potentially use ShuntingYard class
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val inputs = args(0).split(',')
    val outputPath = new File(args(1)).getAbsolutePath
  }
}
