package gaia

import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import geotrellis.raster.resample._
import geotrellis.raster.reproject._
import geotrellis.proj4._

import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.file._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index._
import geotrellis.spark.pyramid._
import geotrellis.spark.reproject._
import geotrellis.spark.tiling._
import geotrellis.spark.render._

import geotrellis.vector._

import org.apache.spark._
import org.apache.spark.rdd._

import java.io.File

object IngestImage {


	def main(args: Array[String]) : Unit = {

		// Get the input and output file locations
		val inputPath = new File(args(0)).getAbsolutePath
		val outputPath = new File(args(1)).getAbsolutePath

	    val conf =
	      new SparkConf()
	        .setMaster("local[*]")
	        .setAppName("Spark Tiler")
	        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	        .set("spark.kryo.registrator", "geotrellis.spark.io.kryo.KryoRegistrator")

	    val sc = new SparkContext(conf)

	    // Do the ingestion
	    run(sc, inputPath, outputPath)

	    // Stop the service
	    sc.stop()
	}


    def fullPath(path: String) = new java.io.File(path).getAbsolutePath


    def run(implicit sc: SparkContext, inputPath: String, outputPath: String) {
	
	    // Read the geotiff in as a single image RDD,
	    // using a method implicitly added to SparkContext by
	    // an implicit class available via the
	    // "import geotrellis.spark.io.hadoop._ " statement.
	    val inputRdd: RDD[(ProjectedExtent, MultibandTile)] =
	      sc.hadoopMultibandGeoTiffRDD(inputPath)   	
    
	    // Use the "TileLayerMetadata.fromRdd" call to find the zoom
	    // level that the closest match to the resolution of our source image,
	    // and derive information such as the full bounding box and data type.
	    val (_, rasterMetaData) =
	      TileLayerMetadata.fromRdd(inputRdd, FloatingLayoutScheme(512))

	    // Use the Tiler to cut our tiles into tiles that are index to a floating layout scheme.
	    // We'll repartition it so that there are more partitions to work with, since spark
	    // likes to work with more, smaller partitions (to a point) over few and large partitions.
	    val tiled: RDD[(SpatialKey, MultibandTile)] =
	      inputRdd
	        .tileToLayout(rasterMetaData.cellType, rasterMetaData.layout, Bilinear)
	        .repartition(100)

	    // We'll be tiling the images using a zoomed layout scheme
	    // in the web mercator format (which fits the slippy map tile specification).
	    // We'll be creating 256 x 256 tiles.
	    val layoutScheme = ZoomedLayoutScheme(WebMercator, tileSize = 256)

	    // We need to reproject the tiles to WebMercator
	    val (zoom, reprojected): (Int, RDD[(SpatialKey, MultibandTile)] with Metadata[TileLayerMetadata[SpatialKey]]) =
	      MultibandTileLayerRDD(tiled, rasterMetaData)
	        .reproject(WebMercator, layoutScheme, Bilinear)

	    // Create the attributes store that will tell us information about our catalog.
	    val attributeStore = FileAttributeStore(outputPath)

	    // Create the writer that we will use to store the tiles in the local catalog.
	    val writer = FileLayerWriter(attributeStore)

	    // Pyramiding up the zoom levels, write our tiles out to the local file system.
	    Pyramid.upLevels(reprojected, layoutScheme, zoom, Bilinear) { (rdd, z) =>
	      val layerId = LayerId("gaia", z)
	      // If the layer exists already, delete it out before writing
	      if(attributeStore.layerExists(layerId)) {
	        new FileLayerManager(attributeStore).delete(layerId)
	      }
	      writer.write(layerId, rdd, ZCurveKeyIndexMethod)
	    }
    }


}