module SimpleDFutils

# collection of DataFrame utilities

using DataFrames
using SQLite,StatsKit

export loadtblSQLite , selectSQLite
export typetally, properdf!
export actionitems!

"""
    properdf!()

    This function will accept a DataFrame and replace nothing with missing,
        map missing-alias values, and simplify the column element type(s).
        The optional named-parameter 'missingalias' is a Vector of elements
        to be mapped to missing (default = ["","NA","na"]).
        
    Example
    ```
    properdf!(mydf)
    ```

"""
function properdf!(df::DataFrame ;  missingalias=["","NA","na"])
    allowmissing!(df)
    # replace nothing's
    df .= ifelse.(isnothing.(df), missing, df)
    # map missingalias
    df .= ifelse.(ifelse.(ismissing.(df .∈ [missingalias]),true, df .∈ [missingalias]), missing,df)
    # reset column Types
    for c in 1: ncol(df)
        type = eltype(df[!,c])
        types = unique(typeof.(df[!,c]))
        hasmissing = ( Missing in types)
        if type == Any
            if (!hasmissing )
                types = filter(x -> x!=Missing, types)
            end
            df[!,c]=convert(AbstractVector{Union{types...}}, df[!,c])
        else
            if (!hasmissing)
                types = filter(x -> x!=Missing, types)
                df[!,c]=convert(AbstractVector{Union{types...}}, df[!,c])
            end
        end
    end
end

"""
    loadtblSQLite()

    This function will load a SQLite table in to a DataFrame object.
    Missing alias strings are converted to Julia type Missing.

    Example
    ```
        mydf=loadtblSQLite("/Users/myaccount/dbfolder/dbfile.db","mytable")
        mydf2=loadtblSQLite("~/dbfile.db","mytable2", missingalias=["none"])
    ```

"""
function loadtblSQLite( dbfile,tblname ;  missingalias=["","NA","na"])
    if (isfile(dbfile))
        dbcon = SQLite.DB(dbfile) 
        tbls=getfield.( SQLite.tables(dbcon), :name)  
        if (tblname in tbls)   
            sql="select * from " * tblname
            df= DBInterface.execute(dbcon, sql) |> DataFrame 
            properdf!(df , missingalias=missingalias)
            return df
        else
            # table not found
            msg="Table not found: " * tblname
            error(msg)
        end
    else
        msg="Database file not found: " * dbfile
        error(msg)
    end
end

"""
    selectSQLite()

    This function will execute a 'select' statement in to a SQLite
        database and place result in to a DataFrame object.
    Missing alias strings are converted to Julia type Missing.

    Example
    ```
        mydf=selectSQLite("/Users/myaccount/dbfolder/dbfile.db","select * from mytable where field1="yes"")
        mydf2=selectSQLite("~/dbfile.db","select * from mytable2", missingalias=["none"])
    ```

"""
function selectSQLite( dbfile,sqlselect ;  missingalias=["","NA","na"])
    if (isfile(dbfile))
        dbcon = SQLite.DB(dbfile) 
        if (occursin("select ",lowercase(sqlselect)))   
            df= DBInterface.execute(dbcon, sqlselect) |> DataFrame 
            proper!(df, missingalias=missingalias)
            return df
        else
            # select not found in query
            msg="Non-valid select statement"
            error(msg)
        end
    else
        msg="Database file not found: " * dbfile
        error(msg)
    end
end


"""
    typetally()

    This function will scan a DataFrame for element types. The return 
        DataFrame will have a row for each column in the scanned object.
        The first column consists of the column names in the scanned object.  
        Each emaining column indicates a unique Type in the scanned object.
        Each entry of the return DataFrame is a 'tuple' cross referencing the 
        scanned column with a corresponding Type. The first tuple entry
        is the total number of elements; the second entry is total number 
        of unique elements.

    Example
    ```
    dftally=typetally(mydf)
    ```
        
"""
function typetally(df::DataFrame)
    # establish parameters of 'df'
    sizedf=size(df)
    # exclude empty datafram from processing
    if sizedf[1]>0
        tarray= zeros(Int,sizedf)
        celltypes=Vector{DataType}(undef, 0)
        celltypenames=Vector{String}(undef, 0)
        ntypes=0
        for r = 1:sizedf[1], c = 1:sizedf[2]
            typecell=typeof(df[r,c])
            typename=string(typecell)
            if ntypes>0
                #check to see if celltypenames is a new type
                pos=findfirst(==(typename),celltypenames)
                if isnothing(pos)
                    # new type
                    push!(celltypes,typecell)
                    push!(celltypenames,typename)
                    ntypes=length(celltypenames)
                    tarray[r,c]=ntypes
                else
                    # not new type
                    tarray[r,c]=pos
                end
            else
                push!(celltypes,typecell)
                push!(celltypenames,typename)
                ntypes=1
                tarray[r,c]=1
            end
        end
        # type of each element recorded
        # tally types by column
        coltally=fill([0,0], (sizedf[2],ntypes))

        # create unique data
        xcol=0
        for col in eachcol(tarray)
            temptally=zeros(Int,ntypes)
            xcol += 1
            for e=1:sizedf[1]
                temptally[col[e]] += 1
            end
            for i2=1:ntypes
                coltally[xcol,i2 ] = [temptally[i2] , coltally[xcol,i2 ][2]  ]
            end
            
            # calulate uniques by type
            for el=1:ntypes
                if coltally[xcol,el][1]>0
                    if celltypenames[el]=="Nothing"  || celltypenames[el]=="Missing"
                        coltally[xcol,el] = [ coltally[xcol,el][1] ,1]
                    else
                        itype= findall(==(el),col)
                        eldf=df[itype,xcol]
                        try  # presort speeds up unique()
                            sort!(eldf)
                        catch
                            # fail to sort. pass eldf unsorted
                        end
                        nel=length(unique(eldf))
                        coltally[xcol,el] = [ coltally[xcol,el][1] ,nel]
                    end
                end
            end  # end create unique
        end
        tallydf= DataFrame(coltally , :auto)
        DataFrames.rename!(tallydf,Symbol.(celltypenames))
        insertcols!(tallydf, 1, :ColNames => names(df))
    else
        msg="checktypesDF() requires a non-empty DataFrame object"
        error(msg)   
    end
    return(tallydf)
end

"""
    actionitems!()

    This function is still under development.  It returns a DataFrame with four
        columns. First, scanned object column name.  Second, category of action.
        Third, consideration (i.e. description of concern).  Fourth, comment listing
        remedy options.
    A side effect of this function is to convert 'nothing' to 'missing' and map
        missing-alias to 'missing' [via properdf!()]. This behavior can be suppressed 
        by setting 'makeproper' to false. The optional named parameter 'missingalias'
        is a Vector of missing-alias values (default=["","NA","na"])

    Example
    ```
    todolistdf=actionitems(mydf)
    ```

"""
function actionitems!(df::DataFrame; makeproper::Bool=true, missingalias=["","NA","na"])
    # function to inspect DataFram and generate DataFrame with 
    # suggested 'clean-up' actions.
    # suggestions are in category resolve type, manage missing, outlier detection

    if (makeproper)
        properdf!(df)
    end
    tallydf = typetally(df)

    ncols=nrow(tallydf)
    typenames=names(tallydf)[2:end]
    ntypes=length(typenames)
    ntotal=nrow(df)
    
    imiss=findfirst(==("Missing"),names(tallydf))
    if isnothing(imiss)
        imiss=0
    end
    pidx=collect(2:(ntypes+1))
    filter!(x->x!=imiss , pidx)
    nonmissnames=names(tallydf)[pidx]

    # create empty action DataFrame"
    #= actions = DataFrame( Column = "Column Names Below:" , 
                            Category = "Category of Action" ,
                            Consideration = "Consider ..." ,
                            Comment = "Comment:" ) =#
    actions = DataFrame( Column = "" , 
                            Category = "" ,
                            Consideration = "" ,
                            Comment = "" )
    
    deleteat!(actions,1)
    # Loop through each column and generate actions
    for curcol=1:ncols
        colname=tallydf[curcol,1]
        datavec= tallydf[curcol,pidx]
        curnames=names(tallydf)[pidx]
        curtally= collect( v[1] for v in datavec )
        curunique= collect( v[2] for v in datavec )
        curntypes= count(x->x>0 , curtally)
        if imiss==0
            nmiss=0
        else
            nmiss=collect( v[1] for v in tallydf[curcol,imiss] )[1]
        end
        

        # 'Type' actions
        if curntypes>1
        curcategory="type overload"
        mtypes = curnames[findall(x->x>0 , curtally)]
        curconsider="There are $(curntypes) Types in the column: $(colname)\n" *
                    "Types: [ " * join(mtypes, " , ") * " ]."
        curcomment="Determine correct Type and plan conversion."
        push!(actions,(colname,curcategory,curconsider,curcomment))
        end
        if curntypes==1
            curcategory="type variance"
            it=findfirst(x -> x>0 , curtally)
            xtally=curtally[it]
            xunique=curunique[it]
            r=1.0*xunique/xtally
            
            if xunique==1
                curconsider="There is only 1 value in column: $(colname)" 
                curcomment="Column many need elimination."
                push!(actions,(colname,curcategory,curconsider,curcomment))
            elseif (r>=.98)
                curconsider="Entries almost all unique"
                curcomment="Can be seen with 'Identity' column, " * 
                "free form text, or DateTime displaed as text/number"
                push!(actions,(colname,curcategory,curconsider,curcomment))
            else
             #placeholder for futher type analyses   
            end


        end

        # 'Missing' actions
        curcategory="missing present"
        if imiss>0 && nmiss>0
            curconsider="There $(nmiss) (of $(ntotal)) 'missing' values."
            curcomment="Consider no action, delete column, delete row, or \n" * 
                        "imputation [mean, median, mode, random, " * 
                        "nearest neighbor, or regression]."
            push!(actions,(colname,curcategory,curconsider,curcomment))
        end

        # 'Outlier' actions
        curcategory="outlier detection"
        # curdata=skippmissing(df[:,colname])  #can use in outlier section if needed



    end


    return(actions)
end

end # end of module
