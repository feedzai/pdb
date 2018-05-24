/*
 * Copyright 2014 Feedzai
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.feedzai.commons.sql.abstraction.engine;

import com.feedzai.commons.sql.abstraction.engine.configuration.PdbProperties;
import com.google.common.base.Throwables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.util.Providers;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.Constructor;
import java.util.Properties;

/**
 * Factory used to obtain database connections.
 *
 * @author Rui Vilao (rui.vilao@feedzai.com)
 * @since 2.0.0
 */
public final class DatabaseFactory {

    /**
     * Hides the public constructor.
     */
    private DatabaseFactory() {
    }

    /**
     * Gets a database connection from the specified properties.
     *
     * @param p The database properties.
     * @return A reference of the specified database engine.
     * @throws DatabaseFactoryException If the class specified does not exist or its not well implemented.
     */
    public static DatabaseEngine getConnection(Properties p) throws DatabaseFactoryException {
        PdbProperties pdbProperties = new PdbProperties(p, true);
        final String engine = pdbProperties.getEngine();

        if (StringUtils.isBlank(engine)) {
            throw new DatabaseFactoryException("pdb.engine property is mandatory");
        }

        try {
            Class<?> c = Class.forName(engine);

            Constructor cons = c.getConstructor(PdbProperties.class);
            final AbstractDatabaseEngine de = (AbstractDatabaseEngine) cons.newInstance(pdbProperties);

            Class<? extends AbstractTranslator> tc = de.getTranslatorClass();

            if (pdbProperties.isTranslatorSet()) {
                final Class<?> propertiesTranslator = Class.forName(pdbProperties.getTranslator());
                if (!AbstractTranslator.class.isAssignableFrom(propertiesTranslator)) {
                    throw new DatabaseFactoryException("Provided translator does extend from AbstractTranslator.");
                }

                tc = (Class<? extends AbstractTranslator>) propertiesTranslator;
            }


            final Injector injector = Guice.createInjector(
                    new PdbModule.Builder()
                            .withTranslator(tc)
                            .withPdbProperties(pdbProperties)
                            .build());
            injector.injectMembers(de);

            return de;
        } catch (final DatabaseFactoryException e) {
            throw e;
        } catch (final Exception e) {
            throw new DatabaseFactoryException(e);
        }
    }

    /**
     * Module that enables dependency injection in PDB.
     */
    public static class PdbModule extends AbstractModule {
        /**
         * The translator.
         */
        private final Class<? extends AbstractTranslator> translator;
        /**
         * The properties.
         */
        private final PdbProperties pdbProperties;

        /**
         * Creates a new instance of {@link PdbModule}.
         *
         * @param translator    The translator.
         * @param pdbProperties The PDB properties.
         */
        private PdbModule(Class<? extends AbstractTranslator> translator, PdbProperties pdbProperties) {
            this.translator = translator;
            this.pdbProperties = pdbProperties;
        }

        @Override
        protected void configure() {
            try {
                bind(PdbProperties.class).toProvider(Providers.of(pdbProperties));
                bind(AbstractTranslator.class).toInstance(translator.newInstance());
            } catch (final Exception e) {
                Throwables.propagate(e);
            }
        }

        /**
         * Builder to create immutable instances of {@link PdbModule}.
         */
        public static class Builder implements com.feedzai.commons.sql.abstraction.util.Builder<PdbModule> {
            private Class<? extends AbstractTranslator> translator;
            private PdbProperties pdbProperties;

            /**
             * Sets the translator class.
             *
             * @param translator The translator class.
             * @return This builder.
             */
            public Builder withTranslator(Class<? extends AbstractTranslator> translator) {
                this.translator = translator;

                return this;
            }

            /**
             * Sets the PDB properties.
             *
             * @param pdbProperties The PDB properties.
             * @return This builder.
             */
            public Builder withPdbProperties(PdbProperties pdbProperties) {
                this.pdbProperties = pdbProperties;

                return this;
            }

            @Override
            public PdbModule build() {
                return new PdbModule(translator, pdbProperties);
            }
        }
    }
}
