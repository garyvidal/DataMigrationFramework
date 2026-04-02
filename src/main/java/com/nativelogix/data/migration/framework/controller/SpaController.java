package com.nativelogix.rdbms2marklogic.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * Forwards all non-file, non-API routes to index.html so React Router
 * can handle client-side navigation.
 *
 * Spring MVC picks more-specific patterns (e.g. /v1/**) first, so API
 * controllers are never affected by this catch-all.
 */
@Controller
public class SpaController {

    /**
     * Matches routes for React Router client-side navigation, but excludes:
     * - /assets/** (static JS/CSS files)
     * - /v1/** (API routes)
     * - Paths with file extensions like .js, .css, .png, etc.
     */
    @RequestMapping(value = {
        "/",
        "/{path:(?!assets|v1|public|static)[^.]*}",
        "/{path:(?!assets|v1|public|static)[^.]*}/**"
    })
    public String forward() {
        return "forward:/index.html";
    }
}
