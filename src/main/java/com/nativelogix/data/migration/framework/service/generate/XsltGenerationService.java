package com.nativelogix.data.migration.framework.service.generate;

import com.nativelogix.data.migration.framework.model.project.DocumentModel;
import com.nativelogix.data.migration.framework.model.project.Project;
import com.nativelogix.data.migration.framework.model.project.ProjectMapping;
import com.nativelogix.data.migration.framework.model.project.XmlNamespace;
import com.nativelogix.data.migration.framework.repository.FileSystemProjectRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

/**
 * Generates a MarkLogic-compatible XSLT 2.0 stylesheet for a project's XML document mapping.
 *
 * <p>No database connection is required — the stylesheet is derived entirely from
 * the project's {@link DocumentModel} mapping metadata.</p>
 */
@Service
@RequiredArgsConstructor
public class XsltGenerationService {

    private final FileSystemProjectRepository projectRepository;
    private final XsltBuilder xsltBuilder;

    /**
     * Generates an XSLT stylesheet string for the project identified by {@code projectId}.
     *
     * @param projectId the project UUID
     * @return XSLT 2.0 string
     * @throws IllegalArgumentException if the project is not found or has no XML mapping
     */
    public String generateXslt(String projectId) throws Exception {
        Project project = projectRepository.findById(projectId)
                .orElseThrow(() -> new IllegalArgumentException("Project not found: " + projectId));

        ProjectMapping mapping = project.getMapping();
        if (mapping == null || mapping.getDocumentModel() == null) {
            throw new IllegalArgumentException("Project has no XML document mapping configured.");
        }

        DocumentModel documentModel = mapping.getDocumentModel();
        if (documentModel.getRoot() == null) {
            throw new IllegalArgumentException("Project XML mapping has no root element defined.");
        }

        List<XmlNamespace> namespaces = mapping.getNamespaces() != null
                ? mapping.getNamespaces() : Collections.emptyList();

        return xsltBuilder.build(documentModel, namespaces);
    }
}
