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


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;


public class HBaseUtil {
    private static final Logger LOGGER = Logger.getLogger(ImageToText.class
            .getName());
    public static void createTable(Configuration conf, String tableName,
                                   String colFamilyPrefix) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName.getBytes());
        HColumnDescriptor colDes = new HColumnDescriptor(colFamilyPrefix);
        tableDescriptor.addFamily(colDes);

        try {
            admin.createTable(tableDescriptor);
            LOGGER.log(Level.INFO, "Created table " + tableName);
        } catch (TableExistsException te) {
            LOGGER.log(Level.SEVERE, "CreateTable threw exception " + te);
        } finally {
            admin.close();
        }

    }

    /*
     * Delete table
     */
    public static void deleteTable(Configuration conf, String tableName)
            throws Exception {
        System.out.println("Disabling table " + tableName);
        HBaseAdmin admin = new HBaseAdmin(conf);

        if (admin.isTableAvailable(tableName)) {
            admin.disableTable(tableName);

            System.out.println("Deleting table " + tableName);
            admin.deleteTable(tableName);
        }

    }

    public static HTable populateDataInMapRDB(Configuration conf, HTable table,
                                              String rowkey, String cf, String col, String value) throws IOException {
        Put p = new Put(Bytes.toBytes(rowkey));
        p.add(Bytes.toBytes(cf), Bytes.toBytes(col), Bytes.toBytes(value));
        table.put(p);
        return table;
    }

    public static void main(String [] args) {
        Configuration config = HBaseConfiguration.create();
        String tableName = "/user/user01/failed_documents";
        String cf = "cf";
        try {

            HBaseUtil.deleteTable(config, "/user/user01/failed_documents");
            HBaseUtil.createTable(config, tableName, cf);

            HTable table = new HTable(config, tableName);
            populateDataInMapRDB(config, table, "errordocument", cf, "error", "Exception - not a jpg");

            table.close();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Creating table threw exception " + e);
        }
    }
}
