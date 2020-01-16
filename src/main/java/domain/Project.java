package domain;

import org.cyberborean.rdfbeans.annotations.RDFBean;

@RDFBean("http://xmlns.com/foaf/0.1/Project")
public class Project {

    private String id;
    private String name;
    private boolean isFinalist;
    private boolean isLimitative;

}

