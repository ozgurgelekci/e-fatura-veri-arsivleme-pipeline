using System.ComponentModel.DataAnnotations;
using InvoiceArchive.Application.Configuration;

namespace InvoiceArchive.Tests;

public class RetentionOptionsValidationTests
{
    [Fact]
    public void Defaults_are_valid_and_safe()
    {
        var opts = new RetentionOptions();
        var errors = Validate(opts);

        Assert.Empty(errors);
        Assert.False(opts.Enabled);
        Assert.True(opts.DryRun);
        Assert.True(opts.RequireS3ObjectExists);
        Assert.Equal(30, opts.MinAgeAfterVerifiedDays);
    }

    [Fact]
    public void Invalid_min_age_fails_validation()
    {
        var opts = new RetentionOptions { MinAgeAfterVerifiedDays = 0 };
        var errors = Validate(opts);

        Assert.Contains(errors, e => e.MemberNames.Contains(nameof(RetentionOptions.MinAgeAfterVerifiedDays)));
    }

    [Fact]
    public void Empty_index_name_fails_validation()
    {
        var opts = new RetentionOptions { IndexName = string.Empty };
        var errors = Validate(opts);

        Assert.Contains(errors, e => e.MemberNames.Contains(nameof(RetentionOptions.IndexName)));
    }

    private static IReadOnlyList<ValidationResult> Validate(RetentionOptions opts)
    {
        var ctx = new ValidationContext(opts);
        var results = new List<ValidationResult>();
        Validator.TryValidateObject(opts, ctx, results, validateAllProperties: true);
        return results;
    }
}
