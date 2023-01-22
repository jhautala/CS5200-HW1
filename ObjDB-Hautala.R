#!/usr/bin/env Rscript
#' CS5200 - HW01: Build Hierarchical Document Database
#' 
#' @description This script runs some local tests around a simple document DB.
#' The source data directory is assumed to be an immediate child of the working
#' directory named 'data'. If no such directory entry is present, we generate
#' a temporary directory, with dummy data. If you want to override this data
#' source, you can pass an alternate path via the optional [data] argument of
#' [executeTests()], or modify the global constant [dataDir]. We execute the
#' tests twice to exercise more execution paths, with final cleanup governed by
#' the global constant [cleanBeforeExit]. You can set this to FALSE if you want
#' to manually review the results of execution (e.g. DB files) after execution.
#' 
#' IMPORTANT: Some of these tests modify files in the current working directory.
#' If the current directory contains a directory named "docDB", some errors
#' may occur (e.g. file permissions) and any prior data therein may be lost.
#' 
#' Error handling consists of output at the point of failure and cancellation
#' upon encountering unexpected warning/error conditions. We try to detect some
#' error conditions, returning NULL/FALSE values to support flow control and
#' to fulfill desired execution to the maximum extent possible.
#' 
#' TODO:
#'  * Improve error handling to use stop/warning and let caller decide
#'    how to handle errors/output? If so, figure out how to document that
#'    behavior, per function.
#'  * Add command line arguments to override some constants:
#'      - 'dataSource' arg to override [dataDir]
#'      - 'cleanBeforeExit' flag to override [cleanBeforeExit]?
#'  * Use [deleteDB()] to fully delete the DB?
#' 
#' @author jhautala

# --- reinitialize the environment
rm(list=ls())


# --- imports
# install non-default packages as needed
if (!("stringr" %in% installed.packages()[,"Package"])) {
  install.packages("stringr")
}
library("stringr")


# --- constants
rootDir <- "docDB"
dataDir <- "data"
fileNames <- c(
  "CampusAtNight #Northeastern #ISEC.jpg",
  "guest-lecture.jpg #Northeastern #Khoury #Systems",
  "some source file.jpg #with #tags"
)
cleanBeforeExit <- FALSE # whether to delete DB contents


# --- functions
#' Print to standard out in the style of [sprintf()]
#'
#' @description
#' `printf` is a simple utility function for printing formatted strings
#' to standard out.
#' 
#' @param s The string to print
#' @param ... Zero or more objects which can be coerced to character; format arguments
#' 
#' @returns None.
printf <- function(s, ...) {
  cat(sprintf(paste(s, "\n", sep=""), ...))
}

#' Split the given `fileName` into tags and base elements
#'
#' @description
#' `toElems` returns a vector of substrings, delimited by any combination of
#' whitespace and full stops.
#' 
#' @param fileName The file name to analyze.
#' 
#' @returns a vector of strings.
toElems <- function(fileName) {
  return(strsplit(fileName, "[\\s\\.]+", perl=TRUE)[[1]])
}

#' Extract tags from given `fileName`
#'
#' @description
#' `getTags` returns a vector of tags, derived from the given `fileName`. Tags
#' are prefixed by a single hash and delimited by any combination of whitespace
#' and full stops.
#' 
#' @param fileName The file name from which to extract tags.
#' 
#' @returns a vector of strings representing tags derived from given `fileName`.
getTags <- function(fileName) {
  elems <- toElems(fileName)
  tag_ii <- grep("^#", elems, perl=TRUE)
  tags <- elems[unlist(tag_ii)]
  return(tags)
}

#' Extract the base file name from the given `fileName` (stripped of tags)
#'
#' @description
#' `getFileName` returns the base file name for the given `fileName`.
#' The base file name consists of substrings, delimited by whitespace
#' and full stops, filtered to exclude substrings prefixed by a hash (i.e.
#' excluding tags), and joined together with full stops.
#' 
#' @param fileName The file name from which to extract the base file name.
#' 
#' @returns a string representing the base file name.
getFileName <- function(fileName) {
  elems <- toElems(fileName)
  fileName_ii <- grep("^(?!#)", elems, perl=TRUE)
  fileName_elems <- elems[unlist(fileName_ii)]
  fileName <- paste(fileName_elems, collapse=".")
  return(fileName)
}

#' Initialize a DB
#'
#' @description
#' `configDB` will initialize a new DB if it is not already initialized.
#' 
#' @param root The directory name wherein to store DB data.
#' @param path The location where the `root` directory will live.
#' 
#' @returns Logical value, where TRUE represents success.
configDB <- function(root=rootDir, path=getwd()) {
  if (!dir.exists(path)) {
    message(sprintf("Invalid path, not found: '%s'", path))
    return(FALSE)
  }
  
  rootPath <- file.path(path, root)
  if (dir.exists(rootPath)) {
    printf(
      "WARNING: Reusing extant directory: '%s'",
      normalizePath(rootPath)
    )
  } else if (file.exists(rootPath)) {
    message(sprintf(
      "ERROR: Unable to create DB! file found at '%s'.",
      normalizePath(rootPath)
    ))
    return(FALSE)
  } else {
    printf("Initializing new storage:\n\t%s", rootPath)
    dir.create(rootPath)
  }
  
  return(TRUE)
}

#' Initialize an object path for the given `tag`
#'
#' @description
#' `genObjPath` will initialize a new object path if it is not already
#' initialized; it will also initialize the DB itself if not already
#' initialized.
#' 
#' @param root The directory name wherein to store DB data.
#' @param tag The tag to initialize.
#' 
#' @returns None.
genObjPath <- function(root, tag) {
  if (is.null(tag)) {
    message(sprintf("Unable to generate path for null tag!"))
    return(NULL)
  }
  
  # config DB as needed (defensive programming > optimizing disk access)
  if (!dir.exists(root)) {
    if (!configDB(root)) {
      message(sprintf("Unable to proceed with path generation!"))
      return(NULL)
    }
  }
  
  sub <- str_replace(tag, "^#", "")
  tagPath <- file.path(root, sub)
  if (!dir.exists(tagPath)) {
    if (file.exists(tagPath)) {
      message(sprintf(
        "Unable to generate object path! File exists: '%s'",
        normalizePath(tagPath)
      ))
      return(NULL)
    }
    
    # NOTE: inline output here makes the specified 'verbose' output messy...
    # printf("Initializing new tag path:\n\t%s", tagPath)
    dir.create(tagPath)
  }
  return(tagPath)
}

#' Ingest the contents of the given `folder` into the DB at `root`
#'
#' @description
#' `storeObjs` copies child files from the given `folder` into the DB at `root`.
#' NOTE: This function ignores "untagged" files, i.e. files whose name does
#' not include a tag.
#' 
#' @param folder The source directory of files to store in the DB.
#' @param root The directory wherein to store DB data.
#' 
#' @returns Logical value, where TRUE represents success.
storeObjs <- function(folder, root=rootDir, verbose=FALSE) {
  # make sure the Source directory exists before doing anything
  if (!dir.exists(folder)) {
    message(sprintf("Source directory does not exist: %s", folder))
    return(FALSE)
  }
  
  # config DB as needed
  if (!dir.exists(root) && !configDB(root)) {
    message(sprintf("Unable to proceed with object ingestion!"))
    return(FALSE)
  }
  
  if (verbose) {
    printf(
      "Importing data:\n\tSource: %s\n\tDestination: %s",
      normalizePath(folder),
      normalizePath(root)
    )
  }
  
  srcFiles <- list.files(folder)
  if (length(srcFiles)) {
    failedTags <- list()
    if (verbose) {
      printf("File Details:")
    }
    # TODO: Extract functions for one or bothe of these loops?
    #       We would need to work out the interface, particularly around
    #       error handling, but also to include the 'verbose' flag...
    for (srcFile in srcFiles) {
      filePath <- file.path(folder, srcFile)
      fileName = getFileName(srcFile)
      for (tag in getTags(srcFile)) {
        tagPath <- genObjPath(root, tag)
        if (is.null(tagPath)) {
          failedTags <- append(failedTags, tag)
          next
        }
        
        dstPath <- file.path(tagPath, fileName)
        if (file.exists(dstPath)) {
          printf("\toverwriting extant directory entry at path '%s'", dstPath)
          if (dir.exists(dstPath)) {
            unlink(dstPath, recursive=TRUE)
          }
        }
        if (verbose) {
          printf(
            "\tcopying '%s' to '%s'",
            srcFile,
            file.path(basename(tagPath), fileName)
          )
        }
        
        file.copy(filePath, dstPath, overwrite=TRUE)
      }
    }
    if (length(failedTags) > 0) {
      message(sprintf(
        "Unable to index the following tags at root '%s':\n\t%s",
        root,
        paste(unique(failedTags), collapse="\n\t")
      ))
      return(FALSE)
    }
  } else {
    # Source exists but contains no child files
    if (verbose) {
      printf("No files in Source!")
    }
  }
  return(TRUE)
}

#' Delete all documents and tags from the DB at `root`
#'
#' @description
#' `clearDB` deletes all documents and tags from the DB at `root`.
#' 
#' @param root The directory wherein to store DB data.
#' 
#' @returns None.
clearDB <- function(root=rootDir) {
  if (!dir.exists(root)) {
    sprintf("WARNING: no DB found at '%s'; unable clear missing DB.")
    return()
  }
  
  for (sub in list.files(root)) {
    subPath = file.path(root, sub)
    unlink(subPath, recursive=TRUE)
  }
}

#' Completely delete the DB at `root`
#'
#' @description
#' `deleteDB` deletes all documents and tags \emph{and} the `root` directory itself.
#' 
#' @param root The directory wherein DB data is stored.
#' 
#' @returns None.
deleteDB <- function(root=rootDir) {
  unlink(root, recursive=TRUE)
}

#' Verify the contents of the dummy DB (i.e. contaning dummy objects) at `root`
#'
#' @description
#' `verifyDummy` scans the DB at `root` for documents and tags, returning a
#' logical value representing success when TRUE.
#' 
#' @param root The directory wherein DB data is stored.
#' 
#' @returns Logical value, representing success when TRUE.
verifyDummy <- function(root) {
  if (dir.exists(rootDir)) {
    errors <- list()
    for (fileName in fileNames) {
      baseFileName <- getFileName(fileName)
      for (tag in getTags(fileName)) {
        filePath <- file.path(
          rootDir,
          str_replace(tag, "^#", ""),
          baseFileName
        )
        if (!file.exists(filePath)) {
          errors <- append(errors, filePath)
        }
      }
    }
    if (length(errors) > 0) {
      message(sprintf("Failed to ingest files!\n\t%s", paste(errors, collapse="\n\t")))
      return(FALSE)
    } else {
      return(TRUE)
    }
  } else {
    message(sprintf("Failed to initialize DB '%s'", rootDir))
    return(FALSE)
  }
}

#' Test a few simple DB functions
#'
#' @description
#' `executeTests` tries to copy files from the given `data` directory
#' to a DB at the given `root`, initializing storage and tag paths as needed.
#' It will verify that DB contents include expected dummy data if `isDummy`
#' is TRUE. Finally, it proceeds to cleanup after itself if `clean` is TRUE.
#' 
#' @param root The directory wherein DB data is stored. The default is `rootDir`.
#' @param data The directory where source data is located The default is `dataDir`.
#' @param verbose TRUE to print details while copying files. The default is FALSE.
#' @param clean TRUE to delete all DB data. The default is FALSE.
#' @param isDummy TRUE to indicate the DB is the "dummy DB". The default is FALSE.
#' 
#' @returns Logical value, representing success when TRUE.
executeTests <- function(
    root=rootDir,
    data=dataDir,
    verbose=FALSE,
    clean=FALSE,
    isDummy=FALSE
) {
  if (verbose) {
    printf("Ingesting data at DB root '%s' (verbose)...", root)
  }
  if (storeObjs(data, root, verbose=verbose)) {
    if (isDummy) {
      if (verifyDummy(root)) {
        printf("Successfully populated DB!")
      } else {
        message(sprintf("Verification failed for dummy DB!"))
      }
    } else {
      # TODO: add some kind of verification for non-dummy DB? 
      printf("Successfully populated DB!")
    }
    
    # clean up the DB
    if (clean) {
      printf("Cleaning up DB at '%s'...", root)
      clearDB(root)
      
      files <- list.files(root, recursive=TRUE, include.dirs=TRUE)
      if (length(files) == 0) {
        printf("Sucessfully cleaned up DB at '%s'", root)
      } else {
        message(sprintf(
          "Directory entries remaining:\n\t%s",
          paste(files, collapse="\n\t")
        ))
      }
    }
  }
}

#' Print stacktrace to stderr
#'
#' @description
#' `traceHandler` prints a stacktrace for unexpected warnings/errors.
#' 
#' @param cond The warning or error that was not handled during execution.
#' 
#' @returns None.
traceHandler <- function(cond) {
  sink(stderr())
  on.exit(sink(NULL))
  message(sprintf('Unexpected %s:\n%s', class(cond)[1], cond))
  traceback(3,1)
}

#' Create a condition handler to print warning/error conditions to stderr and
#' return an exit code (i.e. the given [code])
#'
#' @description
#' `cancellationHandler` creates a handler to print unexpected warning/error
#' conditions and return an exit code.
#' 
#' @param code The exit code to return.
#' 
#' @returns A condition handler function.
cancellationHandler <- function(code) {
  function(cond) {
    sink(stderr())
    on.exit(sink(NULL))
    message(sprintf(
      "Execution failed due to unexpected %s! See detailed output above.",
      class(cond)[1]
    ))
    return(code)
  }
}

#' Run some tests to confirm DB functionality.
#' 
#' @returns None.
main <- function()
{
  # --- check the local file system for initial conditions prior to testing
  rootExtant <- dir.exists(rootDir)
  dataExtant <- dir.exists(dataDir)
  # use tryCatch to be sure we clean up dummy data as needed when finished
  status <- tryCatch(
    {
      # use local handlers to get useful stacktraces
      withCallingHandlers(
        {
          # --- initialize the file system as needed for testing
          if (rootExtant) {
            printf('Default DB root found; proceeding to "take ownership"...')
          }
          if (!dataExtant) {
            # create the data directory and source data as needed
            dir.create(dataDir)
            for (fileName in fileNames) {
              filePath = file.path(dataDir, fileName)
              file.create(filePath)
            }
          }
          
          # --- test execution
          # test all functions, including cleanup
          executeTests(verbose=TRUE, clean=TRUE, isDummy=!dataExtant)
          # test all functions without verbose output
          if (!cleanBeforeExit) {
            suffix <- " (preserving DB files, per global constant 'cleanBeforeExit')"
          } else {
            suffix <- ""
          }
          printf("Re-populating DB without 'verbose' output%s...", suffix)
          executeTests(clean=cleanBeforeExit, isDummy=!dataExtant)
        },
        warning=traceHandler,
        error=traceHandler
      )
      return(0)
    },
    warning=cancellationHandler(2),
    error=cancellationHandler(1),
    finally={
      # --- clean up local changes (e.g. delete dummy DB)
      if (!dataExtant) {
        printf("Cleaning up dummy data in '%s'...", dataDir)
        unlink(dataDir, recursive=TRUE)
        printf("Dummy data deleted.")
      }
    }
  )
  
  # print exit status
  if (status != 0) {
    sink(stderr())
    on.exit(sink(NULL))
  }
  printf('return status: %s', status)
  return(status)
}


# --- main execution
status <- main()
if (!interactive()) {
  quit(status=status) # TODO: quit with non-zero exit code on error!
}
