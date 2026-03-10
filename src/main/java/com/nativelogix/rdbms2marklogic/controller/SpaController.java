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

    @RequestMapping(value = {"/{path:[^\\.]*}", "/{path:[^\\.]*}/**"})
    public String forward() {
        return "forward:/index.html";
    }
}
