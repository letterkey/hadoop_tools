package com.oneapm.hadoop.job;

import org.apache.hadoop.util.ProgramDriver;

/**
 * Created by lifeiyf on 2015/7/13.
 */
public class Driver {
    public static void main(String[] args) throws Throwable {
        ProgramDriver pgd = new ProgramDriver();
        pgd.addClass(CrushFile.NAME, CrushFile.class, "crush file.");
        ProgramDriver.class.getMethod("driver", new Class [] {String[].class}).
                invoke(pgd, new Object[]{args});
    }
}
