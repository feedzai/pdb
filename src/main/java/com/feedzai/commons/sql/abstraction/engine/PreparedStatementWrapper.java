/*
 * The copyright of this file belongs to Feedzai. The file cannot be
 * reproduced in whole or in part, stored in a retrieval system,
 * transmitted in any form, or by any means electronic, mechanical,
 * photocopying, or otherwise, without the prior permission of the owner.
 *
 * (c) 2021 Feedzai, Strictly Confidential
 */
package com.feedzai.commons.sql.abstraction.engine;

import lombok.Builder;
import lombok.Getter;

import java.sql.PreparedStatement;

/**
 * Wrapper class for the {@link PreparedStatement} which adds more information
 * like the index of the last bind parameter that was filled in the prepared statement.
 *
 * @author Tiago Silva (tiago.silva@feedzai.com)
 * @since 2.8.3
 */
@Builder
public class PreparedStatementWrapper {
    /**
     * The {@link PreparedStatement} to use in the operation.
     */
    @Getter
    PreparedStatement preparedStatement;

    /**
     * The position (1-based) of the last bind parameter that was filled in the prepared statement.
     */
    @Getter
    int lastBindPosition;
}
