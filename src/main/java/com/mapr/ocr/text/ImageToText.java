/*
 *  Copyright 2009-2016 MapR Technologies
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package com.mapr.ocr.text;

import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.input.PortableDataStream;
import org.ghost4j.document.PDFDocument;
import org.ghost4j.renderer.SimpleRenderer;
import scala.Tuple2;

import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.mapr.ocr.text.HBaseUtil.populateDataInMapRDB;


public class ImageToText implements Serializable {
    private static final Logger LOGGER = Logger.getLogger(ImageToText.class
            .getName());
    static JavaSparkContext jsc = null;
    static Configuration config = HBaseConfiguration.create();
    static String convertedTableName = "/user/user01/datatable";
    static String errorTableName = "/user/user01/failed_documents";
    static HTable convertedTable = null;
    static HTable errorTable = null;
    static String cf = "cf";


    public static String processPDF(String fileName) {
        File imageFile = new File(fileName);
        StringBuilder resultText = new StringBuilder();
        PDFDocument pdfDocument = new PDFDocument();
        try {
            pdfDocument.load(imageFile);
            SimpleRenderer simpleRenderer = new SimpleRenderer();
            simpleRenderer.setResolution(300);

            List<Image> images = simpleRenderer.render(pdfDocument);

            ITesseract tesseract = new Tesseract();
            tesseract.setLanguage("eng");
            for (Image x : images) {
                resultText.append(tesseract.doOCR((BufferedImage) x));
            }

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Exception processing PDF file  " + fileName + " " + e);
            String rowKey = FilenameUtils.removeExtension(fileName.substring(fileName.lastIndexOf("/")+1, fileName.length()));
            populateDataInMapRDB(config, errorTable, rowKey, cf, "error", e.toString());
            populateDataInMapRDB(config, errorTable, rowKey, cf, "filepath", fileName);
        } finally {
            return resultText.toString();
        }

    }



    public static String processImageFile(String fileName) {
        Tesseract instance = new Tesseract(); // JNA Interface Mapping
        File imageFile = new File(fileName);
        String resultText = null;

        instance.setLanguage("eng");

        try {
            resultText = instance.doOCR(imageFile);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.log(Level.SEVERE, "Exception processing image file  " + fileName + " " + e.toString());
            String rowKey = FilenameUtils.removeExtension(fileName.substring(fileName.lastIndexOf("/")+1, fileName.length()));
            populateDataInMapRDB(config, errorTable, rowKey, cf, "error", e.toString());
            populateDataInMapRDB(config, errorTable, rowKey, cf, "filepath", fileName);
        } finally {
            return resultText;
        }
    }

    public static void init() {
        SparkConf conf = new SparkConf().setAppName("ImageToText").setMaster("local");
        jsc = new JavaSparkContext(conf);
    }

    public static void processFile(String fileName) {
        System.out.println("Processing file:" + fileName);

        File imageFile = new File(fileName);
        String resultText = null;

        if (fileName.endsWith("pdf")) {
            resultText = processPDF(fileName);
        } else {
            resultText = processImageFile(fileName);
        }

        try {
            String rowKey = FilenameUtils.removeExtension(fileName.substring(fileName.lastIndexOf("/")+1, fileName.length()));
            populateDataInMapRDB(config, convertedTable, rowKey, cf, "info", resultText);
            populateDataInMapRDB(config, convertedTable, rowKey, cf, "filepath", fileName);
        }catch(Exception ex) {
            ex.printStackTrace();
            LOGGER.log(Level.SEVERE, "Exception processing file  " + fileName + " " + ex);
        }

        LOGGER.info(resultText);

    }

    public static void main(String[] args) throws IOException{
        String inputPath = "/user/user01/images";
        boolean createTable = false;

        if (args.length > 0) {
            inputPath = args[0];
            if (args.length > 1 && args[1].equals("CreateTable")) {
                createTable = true;
            }
        }

        convertedTable =new HTable(config, convertedTableName);
        errorTable =new HTable(config, errorTableName);

        if (createTable) {
            try {

                HBaseUtil.deleteTable(config, "/user/user01/datatable");
                HBaseUtil.createTable(config, convertedTableName, cf);

                HTable table = new HTable(config, convertedTableName);
                populateDataInMapRDB(config, table, "rowkey2", cf, "colname", "colval");

                table.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Creating table threw exception " + e);
            }
        }

        ImageToText imageToText = new ImageToText();
        imageToText.init();
        imageToText.startReadingFiles(inputPath);

        //cleanup
        convertedTable.close();
        errorTable.close();


    }



    public void startReadingFiles(String inputPath) {

        JavaPairRDD<String, PortableDataStream> readRDD = jsc.binaryFiles(inputPath);

        final StringBuilder resultText = new StringBuilder();

        readRDD.map(new Function<Tuple2<String, PortableDataStream>, String>() {
            @Override
            public String call(Tuple2<String, PortableDataStream> pair) {
                LOGGER.log(Level.INFO, "Read File " + pair._2.getPath());
                LOGGER.log(Level.INFO, "PDF Document " + resultText);
                //Remove the file: prefix
                String fileName =  StringUtils.remove(pair._2.getPath(), "maprfs://");

                processFile(StringUtils.remove(fileName, "file:"));
                return null;
            }
        }).collect();

    }
}
