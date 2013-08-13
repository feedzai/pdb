/*
 *  The copyright of this file belongs to FeedZai SA. The file cannot be    *
 *  reproduced in whole or in part, stored in a retrieval system,           *
 *  transmitted in any form, or by any means electronic, mechanical,        *
 *  photocopying, or otherwise, without the prior permission of the owner.  *
 *
 * (c) 2013 Feedzai SA, Rights Reserved.
 */
package com.feedzai.commons.sql.abstraction.engine.impl.abs;

import org.junit.Ignore;
import java.io.Serializable;

/**
 * @author deag
 */
@Ignore
public class BlobTest implements Serializable {
    public int id;
    public String name;

    public BlobTest(int id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public String toString() {
        return "BlobTest{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BlobTest)) return false;

        BlobTest blobTest = (BlobTest) o;

        if (id != blobTest.id) return false;
        if (name != null ? !name.equals(blobTest.name) : blobTest.name != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        return result;
    }
}