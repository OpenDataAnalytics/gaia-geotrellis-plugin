package gaia

import com.typesafe.config.ConfigFactory
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import java.io.File

object CreateNDVI {

  /**
    * Caclulate NDVI from a multiband image (IR, Near-IR).
    * @param args: an array of strings representing
    *            the following arguments: input,
    *            output, bands array as string ('1,2')
    *
    *
    */
  def main(args: Array[String]): Unit = {
    val inputImg = new File(args(0)).getAbsolutePath
    val ndviPath = new File(args(1)).getAbsolutePath
    val bands = args(2).split(',')
    val redBand = bands(0).toInt
    val irBand = bands(1).toInt

    val tifinput = MultibandGeoTiff(inputImg)
    val ndvi = {
      // Convert the tile to type double values,
      // because we will be performing an operation that
      // produces floating point values.
      val tile = tifinput.convert(DoubleConstantNoDataCellType)

      // Use the combineDouble method to map over the red and infrared values
      // and perform the NDVI calculation.
      tile.combineDouble(redBand, irBand) { (r: Double, ir: Double) =>
        if(isData(r) && isData(ir)) {
          (ir - r) / (ir + r)
        } else {
          Double.NaN
        }
      }
    }

    // Save the NDVI to geotiff
    SinglebandGeoTiff(ndvi, tifinput.extent, tifinput.crs).write(ndviPath)
  }
}
