package org.troisil.datamining.utils;

import lombok.Builder;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Builder
public class Tweet {
    private final String username;
    private final String content;
    private final String createdAt;
}