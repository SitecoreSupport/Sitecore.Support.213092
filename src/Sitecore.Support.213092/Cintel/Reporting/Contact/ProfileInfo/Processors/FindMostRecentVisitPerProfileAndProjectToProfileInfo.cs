using Sitecore.Cintel.Commons;
using Sitecore.Cintel.Reporting;
using Sitecore.Cintel.Reporting.Contact.ProfileInfo;
using Sitecore.Cintel.Reporting.Processors;
using Sitecore.Cintel.Reporting.ReportingServerDatasource;
using Sitecore.Globalization;
using System;
using System.Collections.Generic;
using System.Data;

namespace Sitecore.Support.Cintel.Reporting.Contact.ProfileInfo.Processors
{
  public class FindMostRecentVisitPerProfileAndProjectToProfileInfo : ReportProcessorBase
  {
    #region Public methods

    public override void Process(ReportProcessorArgs reportArguments)
    {
      DataTable rawTable = reportArguments.QueryResult;
      rawTable = SortByRecency(rawTable, reportArguments.ReportParameters.ViewName);

      DataTable resultTable = reportArguments.ResultTableForView;

      this.ProjectRawTableIntoResultTable(reportArguments, rawTable, resultTable);

      reportArguments.ResultSet.Data.Dataset[reportArguments.ReportParameters.ViewName] = resultTable;
    }

    #endregion

    #region Private methods

    private static DataTable SortByRecency(DataTable rawTable, string viewName)
    {
      var args = new ReportProcessorArgs(new ViewParameters())
      {
        ReportParameters =
        {
          SortFields = new List<SortCriterion>
          {
            new SortCriterion(XConnectFields.Interaction.ContactVisitIndex, SortDirection.Asc)
          },
          ViewName = viewName
        },
        ResultSet = new ResultSet<DataTable>(0, int.MaxValue)
        {
          Data = new ResultDataContainer<DataTable>
          {
            Dataset = new Dictionary<string, DataTable>
            {
              {
                viewName, rawTable
              }
            }
          }
        }
      };

      var sortProcessor = new ApplySorting();
      sortProcessor.Process(args);

      return args.ResultSet.Data.Dataset[viewName];
    }

    private static bool WasPatternApplied(Guid? patternId)
    {
      return patternId.HasValue;
    }

    private bool ProjectOneProfile(DataTable resultTable, DataRow sourceRow)
    {
      DataRow resultRow = resultTable.NewRow();

      bool fillWasSuccesful = this.TryFillData(resultRow, Schema.ContactId, sourceRow, XConnectFields.Interaction.ContactId)
                              && this.TryFillData(resultRow, Schema.LatestVisitId, sourceRow, XConnectFields.Interaction.Id)
                              && this.TryFillData(resultRow, Schema.ProfileId, sourceRow, XConnectFields.Profile.ProfileId)
                              && this.TryFillData(resultRow, Schema.LatestVisitIndex, sourceRow, XConnectFields.Interaction.ContactVisitIndex)
                              && this.TryFillData(resultRow, Schema.LatestVisitStartDateTime, sourceRow, XConnectFields.Interaction.StartDate)
                              && this.TryFillData(resultRow, Schema.LatestVisitEndDateTime, sourceRow, XConnectFields.Interaction.EndDate)
                              && this.TryFillData(resultRow, Schema.ProfileCount, sourceRow, XConnectFields.Profile.Count);

      var r = sourceRow.Field<Guid?>(XConnectFields.Profile.PatternId);
      resultRow[Schema.PatternWasAppliedToVisit.Name] = WasPatternApplied(r);

      if (!fillWasSuccesful)
      {
        return false;
      }

      resultTable.Rows.Add(resultRow);
      return true;
    }

    private void ProjectRawTableIntoResultTable(ReportProcessorArgs reportArguments, DataTable rawTable, DataTable resultTable)
    {
      bool mandatoryDataMissing = false;

      mandatoryDataMissing = !this.ProjectUniqueProfiles(rawTable, resultTable);

      if (mandatoryDataMissing)
      {
        LogNotificationForView(reportArguments.ReportParameters.ViewName, MandatoryDataMissing);
      }
    }

    private bool ProjectUniqueProfiles(DataTable rawTable, DataTable resultTable)
    {
      bool allRowsProjected = true;
      var processedProfiles = new List<Guid>();

      foreach (DataRow sourceRow in rawTable.AsEnumerable())
      {
        var profileId = sourceRow.Field<Guid>(Schema.ProfileId.Name);

        if (processedProfiles.Contains(profileId))
        {
          continue;
        }

        if (!this.ProjectOneProfile(resultTable, sourceRow))
        {
          allRowsProjected = false;
          continue;
        }
        processedProfiles.Add(profileId);
      }
      return allRowsProjected;
    }

    #endregion

    private static NotificationMessage MandatoryDataMissing
    {
      get
      {
        return new NotificationMessage
        {
          Id = 13,
          MessageType = NotificationTypes.Error,
          Text = Translate.Text("One or more data entries are missing due to invalid data")
        };
      }
    }
  }
}