/////////////////////////////////////////////
// Copyright (c) Gaia Platform LLC
// All rights reserved.
/////////////////////////////////////////////

package com.gaiaplatform.truegraphdb.tinkerpop.gremlin.structure;

import java.util.*;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

public abstract class TrueGraphDBElement implements Element
{
    protected final TrueGraphDBGraph graph;
    protected final Object id;
    protected final String label;
    protected boolean removed = false;

    protected TrueGraphDBElement(final Graph graph, final Object id, final String label)
    {
        this.graph = (TrueGraphDBGraph)graph;
        this.id = id;
        this.label = label;
    }

    public Graph graph()
    {
        return this.graph;
    }

    public Object id()
    {
        return this.id;
    }

    public String label()
    {
        return this.label;
    }

    public int hashCode()
    {
        return ElementHelper.hashCode(this);
    }

    public boolean equals(final Object object)
    {
        return ElementHelper.areEqual(this, object);
    }

    protected static IllegalStateException elementAlreadyRemoved(
        final Class<? extends Element> clazz, final Object id)
    {
        return new IllegalStateException(
            String.format("%s with id %s was already removed.", clazz.getSimpleName(), id));
    }
}