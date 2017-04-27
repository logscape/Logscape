package org.fusesource.scalate.util;

import java.util.*;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;
import org.fusesource.scalate.support.Precompiler;

import java.io.File;

/**
 * Created by NAvery on 15/08/2016.
 */
public class TemplatePrecompiler extends Task {

    private String target;
    private String source;

    public  TemplatePrecompiler() {
        System.out.println("TemplatePrecompiler CREATE");
    }

    public void setTargetDirectory(String target) {
        this.target = target;
    }
    public void setSourceDirectory(String source) {

        this.source = source;
    }
    @Override
    public void execute() throws BuildException {
        try {
            System.out.println("TemplatePrecompiler EXECUTE 1 \n\tTarget:" + target);

            Precompiler precompiler = new Precompiler();


            precompiler.workingDirectory_$eq(new File(System.getProperty("scalate.workdir", "tmp")));
            precompiler.sources_$eq(new File[]{new File(source)});

            System.out.println("\n\tSource:" + precompiler.sources());


            precompiler.targetDirectory_$eq(new File(target));
            System.out.println("TemplatePrecompiler EXECUTE1");

            precompiler.execute();

            System.out.println("TemplatePrecompiler FINISHED  - Templates:" + Arrays.toString(precompiler.templates()));


        } catch (Throwable t) {
            System.out.println("FAILED:" + t);
            t.printStackTrace();
            throw new BuildException(t);
        }
    }
}
